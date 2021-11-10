// Package gfs implements FUSE on top oh grailfile.  Function Main is the entry
// point.
package gfs

import (
	"context"
	"crypto/sha512"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"

	"github.com/grailbio/base/errors"
	"github.com/grailbio/base/file"
	"github.com/grailbio/base/log"
	gunsafe "github.com/grailbio/base/unsafe"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// Inode represents a file or a directory.
type inode struct {
	fs.Inode
	// full pathname, such as "s3://bucket/key0/key1"
	path string
	// dir entry as stored in the parent directory.
	ent fuse.DirEntry

	mu   sync.Mutex // guards the following fields.
	stat cachedStat // TODO: Remove this since we're now using kernel caching.

	// nDirStreamRef tracks the usage of this inode in DirStreams. It is used
	// to decide whether an inode can be reused to service LOOKUP
	// operations. To handle READDIRPLUS, go-fuse interleaves LOOKUP calls for
	// each directory entry. We allow the inode associated with the previous
	// directory entry to be used in LOOKUP to avoid costly API calls.
	//
	// Because an inode can be the previous entry in multiple DirStreams, we
	// maintain a reference count.
	//
	// It is possible for the inode to be forgotten, e.g. when the kernel is
	// low on memory, before the LOOKUP call. If this happens, LOOKUP will not
	// be able to reuse it. This seems to happen rarely, if at all, in
	// practice.
	nDirStreamRef int32
}

// Amount of time to cache directory entries and file stats (size, mtime).
const cacheExpiration = 5 * time.Minute

// RootInode is a singleton inode created for the root mount point.
type rootInode struct {
	inode
	// The context to be used for all file operations. It's vcontext.Background()
	// in Grail environments.
	// TODO(josh): Consider removing and using operation-specific contexts instead (like readdir).
	ctx context.Context
	// Directory for storing tmp files.
	tmpDir string
}

// Handle represents an open file handle.
type handle struct {
	// The file that the handle belongs to
	inode *inode
	// Open mode bits. O_WRONLY, etc.
	openMode uint32
	// Size passed to Setattr, if any. -1 if not set.
	requestedSize int64
	// Remembers the result of the first Flush. If Flush is called multiple times
	// they will return this code.
	closeErrno syscall.Errno

	// At most one of the following three will be set.  Initialized lazily on
	// first Read or Write.
	dw  *directWrite // O_WRONLY|O_TRUNC, or O_WRONLY for a new file.
	dr  *directRead  // O_RDONLY.
	tmp *tmpIO       // everything else, e.g., O_RDWR or O_APPEND.
}

// openMode is a bitmap of O_RDONLY, O_APPEND, etc.
func newHandle(inode *inode, openMode uint32) *handle {
	return &handle{inode: inode, openMode: openMode, requestedSize: -1}
}

// DirectWrite is part of open file handle. It uploads data directly to the remote
// file. Used when creating a new file, or overwriting an existing file with
// O_WRONLY|O_TRUNC.
type directWrite struct {
	fp file.File
	w  io.Writer
	// The next expected write offset. Calling Write on a wrong offset results in
	// error (w doesn't implement a seeker).
	off int64
}

// DirectRead is part of open file handle. It is used when reading a file
// readonly.
type directRead struct {
	fp file.File
	r  io.ReadSeeker
}

// TmpIO is part of open file handle. It writes data to a file in the local file
// system. On Flush (i.e., close), the file contents are copied to the remote
// file. It is used w/ O_RDWR, O_APPEND, etc.
type tmpIO struct {
	fp *os.File // refers to a file in -tmp-dir.
}

// CachedStat is stored in inode and a directory entry to provide quick access
// to basic stats.
type cachedStat struct {
	expiration time.Time
	size       int64
	modTime    time.Time
}

func downCast(n *fs.Inode) *inode {
	nn := (*inode)(unsafe.Pointer(n))
	if nn.path == "" {
		log.Panicf("not an inode: %+v", n)
	}
	return nn
}

var (
	_ fs.InodeEmbedder = (*inode)(nil)

	_ fs.NodeAccesser  = (*inode)(nil)
	_ fs.NodeCreater   = (*inode)(nil)
	_ fs.NodeGetattrer = (*inode)(nil)
	_ fs.NodeLookuper  = (*inode)(nil)
	_ fs.NodeMkdirer   = (*inode)(nil)
	_ fs.NodeOpener    = (*inode)(nil)
	_ fs.NodeReaddirer = (*inode)(nil)
	_ fs.NodeRmdirer   = (*inode)(nil)
	_ fs.NodeSetattrer = (*inode)(nil)
	_ fs.NodeUnlinker  = (*inode)(nil)

	_ fs.FileFlusher  = (*handle)(nil)
	_ fs.FileFsyncer  = (*handle)(nil)
	_ fs.FileLseeker  = (*handle)(nil)
	_ fs.FileReader   = (*handle)(nil)
	_ fs.FileReleaser = (*handle)(nil)
	_ fs.FileWriter   = (*handle)(nil)
)

func newAttr(ino uint64, mode uint32, size uint64, optionalMtime time.Time) (attr fuse.Attr) {
	const blockSize = 1 << 20
	attr.Ino = ino
	attr.Mode = mode
	attr.Nlink = 1
	attr.Size = size
	attr.Blocks = (attr.Size-1)/blockSize + 1
	if !optionalMtime.IsZero() {
		attr.SetTimes(nil, &optionalMtime, nil)
	}
	return
}

// GetModeBits produces the persistent mode bits so that the kernel can
// distinguish regular files from directories.
func getModeBits(isDir bool) uint32 {
	mode := uint32(0)
	if isDir {
		mode |= syscall.S_IFDIR | 0755
	} else {
		mode |= syscall.S_IFREG | 0644
	}
	return mode
}

// GetIno produces a fake inode number by hashing the path.
func getIno(path string) uint64 {
	h := sha512.Sum512_256(gunsafe.StringToBytes(path))
	return binary.LittleEndian.Uint64(h[:8])
}

// GetFileName extracts the filename part of the path. "dir" is the directory
// that the file belongs in.
func getFileName(dir *inode, path string) string {
	if dir.IsRoot() {
		return path[len(dir.path):]
	}
	return path[len(dir.path)+1:] // +1 to remove '/'.
}

func errToErrno(err error) syscall.Errno {
	if err == nil {
		return 0
	}
	log.Debug.Printf("error %v: stack=%s", err, string(debug.Stack()))
	switch {
	case err == nil:
		return 0
	case errors.Is(errors.Timeout, err):
		return syscall.ETIMEDOUT
	case errors.Is(errors.Canceled, err):
		return syscall.EINTR
	case errors.Is(errors.NotExist, err):
		return syscall.ENOENT
	case errors.Is(errors.Exists, err):
		return syscall.EEXIST
	case errors.Is(errors.NotAllowed, err):
		return syscall.EACCES
	case errors.Is(errors.Integrity, err):
		return syscall.EIO
	case errors.Is(errors.Invalid, err):
		return syscall.EINVAL
	case errors.Is(errors.Precondition, err), errors.Is(errors.Unavailable, err):
		return syscall.EAGAIN
	case errors.Is(errors.Net, err):
		return syscall.ENETUNREACH
	case errors.Is(errors.TooManyTries, err):
		log.Error.Print(err)
		return syscall.EINVAL
	}
	return fs.ToErrno(err)
}

// Root reports the inode of the root mountpoint.
func (n *inode) root() *rootInode { return n.Root().Operations().(*rootInode) }

// Ctx reports the context passed from the application when mounting the
// filesystem.
func (n *inode) ctx() context.Context { return n.root().ctx }

// addDirStreamRef adds a single reference to this inode. It must be eventually
// followed by a dropRef.
func (n *inode) addDirStreamRef() {
	_ = atomic.AddInt32(&n.nDirStreamRef, 1)
}

// dropDirStreamRef drops a single reference to this inode.
func (n *inode) dropDirStreamRef() {
	if x := atomic.AddInt32(&n.nDirStreamRef, -1); x < 0 {
		panic("negative reference count; unmatched drop")
	}
}

// previousOfAnyDirStream returns true iff the inode is the previous entry
// returned by any outstanding DirStream.
func (n *inode) previousOfAnyDirStream() bool {
	return atomic.LoadInt32(&n.nDirStreamRef) > 0
}

// Access is called to implement access(2).
func (n *inode) Access(_ context.Context, mask uint32) syscall.Errno {
	// TODO(saito) I'm not sure returning 0 blindly is ok here.
	log.Debug.Printf("setattr %s: mask=%x", n.path, mask)
	return 0
}

// Setattr is called to change file attributes. This function only supports
// changing the size.
func (n *inode) Setattr(_ context.Context, fhi fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	n.mu.Lock()
	defer n.mu.Unlock()

	usize, ok := in.GetSize()
	if !ok {
		// We don't support setting other attributes now.
		return 0
	}
	size := int64(usize)

	if fhi != nil {
		fh := fhi.(*handle)
		switch {
		case fh.dw != nil:
			if size == fh.dw.off {
				return 0
			}
			log.Error.Printf("setattr %s: setting size to %d in directio mode not supported (request: %+v)", n.path, size, in)
			return syscall.ENOSYS
		case fh.dr != nil:
			log.Error.Printf("setattr %s: readonly", n.path)
			return syscall.EPERM
		case fh.tmp != nil:
			return errToErrno(fh.tmp.fp.Truncate(size))
		default:
			fh.requestedSize = size
			return 0
		}
	}

	if size != 0 {
		log.Error.Printf("setattr %s: setting size to nonzero value (%d) not supported", n.path, size)
		return syscall.ENOSYS
	}
	ctx := n.ctx()
	fp, err := file.Create(ctx, n.path)
	if err != nil {
		log.Error.Printf("setattr %s: %v", n.path, err)
		return errToErrno(err)
	}
	if err := fp.Close(ctx); err != nil {
		log.Error.Printf("setattr %s: %v", n.path, err)
		return errToErrno(err)
	}
	return 0
}

func (n *inode) Getattr(_ context.Context, fhi fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	ctx := n.ctx()
	if n.ent.Ino == 0 || n.ent.Mode == 0 {
		log.Panicf("node %s: ino or mode unset: %+v", n.path, n)
	}
	if n.IsDir() {
		log.Debug.Printf("getattr %s: directory", n.path)
		out.Attr = newAttr(n.ent.Ino, n.ent.Mode, 0, time.Time{})
		return 0
	}

	var fh *handle
	if fhi != nil {
		fh = fhi.(*handle)
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if fh != nil {
		if err := fh.maybeInitIO(); err != nil {
			return errToErrno(err)
		}
		if t := fh.tmp; t != nil {
			log.Debug.Printf("getattr %s: tmp", n.path)
			stat, err := t.fp.Stat()
			if err != nil {
				log.Printf("getattr %s (%s): %v", n.path, t.fp.Name(), err)
				return errToErrno(err)
			}
			out.Attr = newAttr(n.ent.Ino, n.ent.Mode, uint64(stat.Size()), stat.ModTime())
			return 0
		}
		if fh.dw != nil {
			out.Attr = newAttr(n.ent.Ino, n.ent.Mode, uint64(n.stat.size), n.stat.modTime)
			return 0
		}
		// fall through
	}
	stat, err := n.getCachedStat(ctx)
	if err != nil {
		log.Printf("getattr %s: err %v", n.path, err)
		return errToErrno(err)
	}
	out.Attr = newAttr(n.ent.Ino, n.ent.Mode, uint64(stat.size), stat.modTime)
	log.Debug.Printf("getattr %s: out %+v", n.path, out)
	return 0
}

func (n *inode) getCachedStat(ctx context.Context) (cachedStat, error) {
	now := time.Now()
	if now.After(n.stat.expiration) {
		log.Debug.Printf("getcachedstat %s: cache miss", n.path)
		info, err := file.Stat(ctx, n.path)
		if err != nil {
			log.Printf("getcachedstat %s: err %v", n.path, err)
			return cachedStat{}, err
		}
		n.stat = cachedStat{
			expiration: now.Add(cacheExpiration),
			size:       info.Size(),
			modTime:    info.ModTime(),
		}
	} else {
		log.Debug.Printf("getcachedstat %s: cache hit %+v now %v", n.path, n.stat, now)
	}
	return n.stat, nil
}

// MaybeInitIO is called on the first call to Read or Write after open.  It
// initializes either the directio uploader or a tempfile.
//
// REQUIRES: fh.inode.mu is locked
func (fh *handle) maybeInitIO() error {
	n := fh.inode
	if fh.dw != nil || fh.dr != nil || fh.tmp != nil {
		return nil
	}
	if (fh.openMode & fuse.O_ANYWRITE) == 0 {
		// Readonly handle should have fh.direct set at the time of Open.
		log.Panicf("open %s: uninitialized readonly handle", n.path)
	}
	if fh.inode == nil {
		log.Panicf("open %s: nil inode: %+v", n.path, fh)
	}
	ctx := n.ctx()
	if (fh.openMode&syscall.O_RDWR) != syscall.O_RDWR &&
		(fh.requestedSize == 0 || (fh.openMode&syscall.O_TRUNC == syscall.O_TRUNC)) {
		// We are fully overwriting the file. Do that w/o a local tmpfile.
		log.Debug.Printf("open %s: direct IO", n.path)
		fp, err := file.Create(ctx, n.path)
		if err != nil {
			return err
		}
		fh.dw = &directWrite{fp: fp, w: fp.Writer(ctx)}
		return nil
	}
	// Do all reads/writes on a local tmp file, and copy it to the remote file on
	// close.
	log.Debug.Printf("open %s: tmp IO", n.path)
	in, err := file.Open(ctx, n.path)
	if err != nil {
		log.Error.Printf("open %s: %v", n.path, err)
		return err
	}
	tmpPath := file.Join(n.root().tmpDir, fmt.Sprintf("%08x", n.ent.Ino))
	tmp, err := os.OpenFile(tmpPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		log.Error.Printf("create %s (open %s): %v", tmpPath, n.path, err)
		_ = in.Close(ctx)
		return errToErrno(err)
	}
	inSize, err := io.Copy(tmp, in.Reader(ctx))
	log.Debug.Printf("copy %s->%s: n+%d, %v", n.path, tmp.Name(), inSize, err)
	if err != nil {
		_ = in.Close(ctx)
		_ = tmp.Close()
		return errToErrno(err)
	}
	if err := in.Close(ctx); err != nil {
		_ = tmp.Close()
		return errToErrno(err)
	}
	now := time.Now()
	n.stat.expiration = now.Add(cacheExpiration)
	n.stat.size = inSize
	n.stat.modTime = now
	fh.tmp = &tmpIO{
		fp: tmp,
	}
	return nil
}

func (fh *handle) Read(_ context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	n := fh.inode
	readDirect := func() (fuse.ReadResult, syscall.Errno) {
		d := fh.dr
		if d == nil {
			return nil, syscall.EINVAL
		}
		log.Debug.Printf("read %s(fh=%p): off=%d seek start", n.path, fh, off)
		newOff, err := d.r.Seek(off, io.SeekStart)
		log.Debug.Printf("read %s(fh=%p): off=%d seek end", n.path, fh, off)
		if err != nil {
			return nil, errToErrno(err)
		}
		if newOff != off {
			log.Panicf("%d <-> %d", newOff, off)
		}

		nByte, err := d.r.Read(dest)
		log.Debug.Printf("read %s(fh=%p): off=%d, nbyte=%d, err=%v", n.path, fh, off, nByte, err)
		if err != nil {
			if err != io.EOF {
				return nil, errToErrno(err)
			}
		}
		return fuse.ReadResultData(dest[:nByte]), 0
	}

	readTmp := func() (fuse.ReadResult, syscall.Errno) {
		t := fh.tmp
		nByte, err := t.fp.ReadAt(dest, off)
		if err != nil {
			if err != io.EOF {
				return nil, errToErrno(err)
			}
		}
		return fuse.ReadResultData(dest[:nByte]), 0
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if err := fh.maybeInitIO(); err != nil {
		//return fuse.ReadResult{}, errToErrno(err)
		return nil, errToErrno(err)
	}
	switch {
	case fh.dr != nil:
		return readDirect()
	case fh.tmp != nil:
		return readTmp()
	default:
		log.Error.Printf("read %s: reading unopened or writeonly file", n.path)
		return nil, syscall.EBADF
	}
}

func (fh *handle) Lseek(ctx context.Context, off uint64, whence uint32) (uint64, syscall.Errno) {
	const (
		// Copied from https://github.com/torvalds/linux/blob/a050a6d2b7e80ca52b2f4141eaf3420d201b72b3/tools/include/uapi/linux/fs.h#L43-L47.
		SEEK_DATA = 3
		SEEK_HOLE = 4
	)
	switch whence {
	case SEEK_DATA:
		return off, 0 // We don't support holes so current offset is correct.
	case SEEK_HOLE:
		stat, err := fh.inode.getCachedStat(ctx)
		if err != nil {
			log.Error.Printf("lseek %s: stat: %v", fh.inode.path, err)
			return 0, errToErrno(err)
		}
		return uint64(stat.size), 0
	}
	log.Error.Printf("lseek %s: unimplemented whence: %d", fh.inode.path, whence)
	return 0, syscall.ENOSYS
}

func (fh *handle) Write(_ context.Context, dest []byte, off int64) (uint32, syscall.Errno) {
	n := fh.inode
	tmpWrite := func() (uint32, syscall.Errno) {
		nByte, err := fh.tmp.fp.WriteAt(dest, off)
		if err != nil {
			log.Error.Printf("write %s: size=%d, off=%d: %v", n.path, len(dest), off, err)
			return 0, errToErrno(err)
		}
		return uint32(nByte), 0
	}

	directWrite := func() (uint32, syscall.Errno) {
		d := fh.dw
		if d.off != off {
			log.Error.Printf("write %s: offset mismatch (expect %d, got %d)", n.path, d.off, off)
			return 0, syscall.EINVAL
		}
		if d.w == nil {
			// closed already
			log.Printf("write %s: already closed", n.path)
			return 0, syscall.EBADF
		}
		nByte, err := d.w.Write(dest)
		if err != nil {
			if nByte > 0 {
				panic(n)
			}
			return 0, errToErrno(err)
		}
		d.off += int64(nByte)
		log.Debug.Printf("write %s: done %d bytes", n.path, nByte)
		return uint32(nByte), 0
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	log.Debug.Printf("write %s: %d bytes, off=%d", n.path, len(dest), off)
	if err := fh.maybeInitIO(); err != nil {
		return 0, errToErrno(err)
	}
	switch {
	case fh.dw != nil:
		return directWrite()
	case fh.tmp != nil:
		return tmpWrite()
	default:
		// file descriptor already closed
		log.Error.Printf("write %s: writing after close", n.path)
		return 0, syscall.EBADF
	}
}

func (fh *handle) Fsync(_ context.Context, _ uint32) syscall.Errno {
	n := fh.inode
	n.mu.Lock()
	defer n.mu.Unlock()
	if d := fh.dw; d != nil {
		n := fh.inode
		// There's not much we can do, but returning ENOSYS breaks too many apps.
		now := time.Now()
		n.stat.expiration = now.Add(cacheExpiration)
		n.stat.size = d.off
		n.stat.modTime = now
		log.Debug.Printf("fsync %s: update stats: stat=%v", n.path, n.stat)
	}
	return 0
}

// Release is called just before the inode is dropped from the kernel memory.
// Return value is unused.
func (fh *handle) Release(_ context.Context) syscall.Errno {
	n := fh.inode
	n.mu.Lock()
	defer n.mu.Unlock()
	switch {
	case fh.tmp != nil:
		if fh.tmp.fp != nil {
			log.Panicf("%s: release called w/o flush", n.path)
		}
	case fh.dw != nil:
		if fh.dw.fp != nil || fh.dw.w != nil {
			log.Panicf("%s: release called w/o flush", n.path)
		}
	default:
		if fh.dr != nil {
			// Readonly handles are closed on the last release.
			_ = fh.dr.fp.Close(n.ctx())
		}
	}
	return 0
}

// Flush is called on close(2). It may be called multiple times when the file
// descriptor is duped.
//
// TODO(saito) We don't support dups now. We close the underlying filestream on
// the first close and subsequent flush calls will do nothing.
func (fh *handle) Flush(_ context.Context) syscall.Errno {
	n := fh.inode
	ctx := n.ctx()

	flushTmpAndUnlock := func() syscall.Errno {
		t := fh.tmp
		mu := &n.mu
		defer func() {
			if mu != nil {
				mu.Unlock()
			}
		}()
		if t.fp == nil {
			mu.Unlock()
			return fh.closeErrno
		}
		out, err := file.Create(ctx, n.path)
		if err != nil {
			log.Error.Printf("flush %s (create): err=%v", n.path, err)
			fh.closeErrno = errToErrno(err)
			_ = t.fp.Close()
			mu.Unlock()
			return fh.closeErrno
		}
		defer func() {
			if out != nil {
				_ = out.Close(ctx)
			}
			if t.fp != nil {
				_ = t.fp.Close()
				t.fp = nil
			}
		}()

		newOff, err := t.fp.Seek(0, io.SeekStart)
		if err != nil {
			log.Error.Printf("flush %s (seek): err=%v", n.path, err)
			fh.closeErrno = errToErrno(err)
			return fh.closeErrno
		}
		if newOff != 0 {
			log.Panicf("newoff %d", newOff)
		}

		nByte, err := io.Copy(out.Writer(ctx), t.fp)
		if err != nil {
			log.Error.Printf("flush %s (copy): err=%v", n.path, err)
			fh.closeErrno = errToErrno(err)
			return fh.closeErrno
		}
		errp := errors.Once{}
		errp.Set(t.fp.Close())
		errp.Set(out.Close(ctx))
		out = nil
		t.fp = nil
		if err := errp.Err(); err != nil {
			fh.closeErrno = errToErrno(err)
			log.Error.Printf("flush %s (close): err=%v", n.path, err)
			return fh.closeErrno
		}

		now := time.Now()
		n.stat.expiration = now.Add(cacheExpiration)
		n.stat.size = nByte
		n.stat.modTime = now

		closeErrno := fh.closeErrno
		mu.Unlock()
		mu = nil
		return closeErrno
	}

	flushDirectAndUnlock := func() syscall.Errno {
		mu := &n.mu
		defer func() {
			if mu != nil {
				mu.Unlock()
			}
		}()
		d := fh.dw
		if d.fp == nil {
			return fh.closeErrno
		}

		err := d.fp.Close(ctx)
		fh.closeErrno = errToErrno(err)
		log.Debug.Printf("flush %s fh=%p, err=%v", n.path, fh, err)
		if d.w != nil {
			now := time.Now()
			n.stat.expiration = now.Add(cacheExpiration)
			n.stat.size = d.off
			n.stat.modTime = now
		}
		d.fp = nil
		d.w = nil
		closeErrno := fh.closeErrno
		mu.Unlock()
		mu = nil
		return closeErrno
	}
	n.mu.Lock()
	switch {
	case fh.tmp != nil:
		return flushTmpAndUnlock()
	case fh.dw != nil:
		return flushDirectAndUnlock()
	}
	n.mu.Unlock()
	return 0
}

// Create is called to create a new file.
func (n *inode) Create(ctx context.Context, name string, flags uint32, mode uint32,
	out *fuse.EntryOut) (*fs.Inode, fs.FileHandle, uint32, syscall.Errno) {
	newPath := file.Join(n.path, name)
	childNode := &inode{
		path: newPath,
		ent: fuse.DirEntry{
			Name: name,
			Ino:  getIno(newPath),
			Mode: getModeBits(false)}}
	childInode := n.NewInode(ctx, childNode, fs.StableAttr{
		Mode: childNode.ent.Mode,
		Ino:  childNode.ent.Ino,
	})
	fh := newHandle(childNode, syscall.O_WRONLY|syscall.O_CREAT|syscall.O_TRUNC)
	fh.requestedSize = 0
	log.Debug.Printf("create %s: (mode %x)", n.path, mode)
	out.Attr = newAttr(n.ent.Ino, n.ent.Mode, 0, time.Time{})
	return childInode, fh, 0, 0
}

// Open opens an existing file.
func (n *inode) Open(_ context.Context, mode uint32) (fs.FileHandle, uint32, syscall.Errno) {
	n.mu.Lock()
	defer n.mu.Unlock()
	ctx := n.ctx()
	if n.IsRoot() {
		// The entries under the root must be buckets, so we can't open it directly.
		log.Error.Printf("open %s: cannot open a file under root", n.path)
		return nil, 0, syscall.EINVAL
	}
	_, dirInode := n.Parent()
	if dirInode == nil {
		log.Panicf("open %s: parent dir does't exist", n.path)
	}
	if (mode & fuse.O_ANYWRITE) == 0 {
		fp, err := file.Open(n.ctx(), n.path)
		if err != nil {
			log.Error.Printf("open %s (mode %x): %v", n.path, mode, err)
			return nil, 0, errToErrno(err)
		}
		fh := newHandle(n, mode)
		fh.dr = &directRead{fp: fp, r: fp.Reader(ctx)}
		log.Debug.Printf("open %s: mode %x, fh %p", n.path, mode, fh)
		return fh, 0, 0
	}

	fh := newHandle(n, mode)
	return fh, 0, 0
}

// FsDirStream implements readdir.
type fsDirStream struct {
	ctx    context.Context
	dir    *inode
	lister file.Lister
	err    error

	seenParent  bool // Whether Next has already returned '..'.
	seenSelf    bool // Whether Next has already returned '.'.
	peekedChild bool // Whether HasNext has Scan()-ed a child that Next hasn't returned yet.

	// previousInode is the inode of the previous entry, i.e. the most recent
	// entry returned by Next.  We hold a reference to service LOOKUP
	// operations that go-fuse issues when servicing READDIRPLUS.  See
	// dirStreamUsage.
	previousInode *fs.Inode
}

// HasNext implements fs.DirStream
func (s *fsDirStream) HasNext() bool {
	s.dir.mu.Lock() // TODO: Remove?
	defer s.dir.mu.Unlock()

	if s.err != nil || s.lister == nil {
		return false
	}
	if !s.seenParent || !s.seenSelf || s.peekedChild {
		return true
	}
	for s.lister.Scan() {
		if getFileName(s.dir, s.lister.Path()) != "" {
			s.peekedChild = true
			return true
		}
		// Assume this is a directory marker:
		// https://web.archive.org/web/20190424231712/https://docs.aws.amazon.com/AmazonS3/latest/user-guide/using-folders.html
		// s3file's List returns these, but empty filenames seem to cause problems for FUSE.
		// TODO: Filtering these in s3file, if it's ok for other users.
	}
	return false
}

// Next implements fs.DirStream
func (s *fsDirStream) Next() (fuse.DirEntry, syscall.Errno) {
	s.dir.mu.Lock()
	defer s.dir.mu.Unlock()

	if s.err != nil {
		return fuse.DirEntry{}, errToErrno(s.err)
	}
	if err := s.lister.Err(); err != nil {
		if _, canceled := <-s.ctx.Done(); canceled {
			s.err = errors.E(errors.Canceled, "list canceled", err)
		} else {
			s.err = err
		}
		return fuse.DirEntry{}, errToErrno(s.err)
	}

	ent := fuse.DirEntry{}
	stat := cachedStat{expiration: time.Now().Add(cacheExpiration)}

	if !s.seenParent {
		s.seenParent = true
		_, parent := s.dir.Parent()
		if parent != nil {
			// Not root.
			parentDir := downCast(parent)
			ent = parentDir.ent
			ent.Name = ".."
			stat = parentDir.stat
			return ent, 0
		}
	}
	if !s.seenSelf {
		s.seenSelf = true
		ent = s.dir.ent
		ent.Name = "."
		stat = s.dir.stat
		return ent, 0
	}
	s.peekedChild = false

	ent = fuse.DirEntry{
		Name: getFileName(s.dir, s.lister.Path()),
		Mode: getModeBits(s.lister.IsDir()),
		Ino:  getIno(s.lister.Path()),
	}
	if info := s.lister.Info(); info != nil {
		stat.size, stat.modTime = info.Size(), info.ModTime()
	}
	inode := s.dir.NewInode(
		s.ctx,
		&inode{path: file.Join(s.dir.path, ent.Name), ent: ent, stat: stat},
		fs.StableAttr{Mode: ent.Mode, Ino: ent.Ino},
	)
	_ = s.dir.AddChild(ent.Name, inode, true)
	s.lockedSetPreviousInode(inode)
	return ent, 0
}

// Close implements fs.DirStream
func (s *fsDirStream) Close() {
	s.dir.mu.Lock()
	s.lockedClearPreviousInode()
	s.dir.mu.Unlock()
}

func (s *fsDirStream) lockedSetPreviousInode(n *fs.Inode) {
	s.lockedClearPreviousInode()
	s.previousInode = n
	s.previousInode.Operations().(*inode).addDirStreamRef()
}

func (s *fsDirStream) lockedClearPreviousInode() {
	if s.previousInode == nil {
		return
	}
	s.previousInode.Operations().(*inode).dropDirStreamRef()
	s.previousInode = nil
}

func (n *inode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	log.Debug.Printf("lookup %s: name=%s start", n.path, name)

	childInode := n.GetChild(name)
	if childInode != nil && childInode.Operations().(*inode).previousOfAnyDirStream() {
		log.Debug.Printf("lookup %s: name=%s using existing child inode", n.path, name)
	} else {
		var (
			childPath = file.Join(n.path, name)
			foundDir  bool
			foundFile cachedStat
			lister    = file.List(ctx, childPath, true /* recursive */)
		)
		// Look for either a file or a directory at this path.
		// If both exist, assume file is a directory marker.
		for lister.Scan() {
			if lister.IsDir() || // We've found an exact match, and it's a directory.
				lister.Path() != childPath { // We're seeing children, so childPath must be a directory.
				foundDir = true
				break
			}
			info := lister.Info()
			foundFile = cachedStat{time.Now().Add(cacheExpiration), info.Size(), info.ModTime()}
		}
		if err := lister.Err(); err != nil {
			if errors.Is(errors.NotExist, err) || errors.Is(errors.NotAllowed, err) {
				// Ignore.
			} else {
				return nil, errToErrno(err)
			}
		}

		if !foundDir && foundFile == (cachedStat{}) {
			log.Debug.Printf("lookup: %s name='%s' not found", n.path, name)
			return nil, syscall.ENOENT
		}

		ent := fuse.DirEntry{
			Name: childPath,
			Mode: getModeBits(foundDir),
			Ino:  getIno(childPath),
		}
		childInode = n.NewInode(
			ctx,
			&inode{path: childPath, ent: ent, stat: foundFile},
			fs.StableAttr{
				Mode: ent.Mode,
				Ino:  ent.Ino,
			})
	}
	ops := childInode.Operations().(*inode)
	out.Attr = newAttr(ops.ent.Ino, ops.ent.Mode, uint64(ops.stat.size), ops.stat.modTime)
	out.SetEntryTimeout(cacheExpiration)
	out.SetAttrTimeout(cacheExpiration)
	log.Debug.Printf("lookup %s name='%s' done: mode=%o ino=%d stat=%+v", n.path, name, ops.ent.Mode, ops.ent.Ino, ops.stat)
	return childInode, 0
}

func (n *inode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	log.Debug.Printf("readdir %s: start", n.path)
	// TODO(josh): Newer Linux kernels (4.20+) can cache the entries from readdir. Make sure this works
	// and invalidates reasonably.
	// References:
	//   Linux patch series: https://github.com/torvalds/linux/commit/69e345511
	//   go-fuse support: https://github.com/hanwen/go-fuse/commit/fa1304749db6eafd8fe64338f10c9750cf693274
	//   libfuse's documentation (describing some kernel behavior): http://web.archive.org/web/20210118113434/https://libfuse.github.io/doxygen/structfuse__lowlevel__ops.html#afa15612c68f7971cadfe3d3ec0a8b70e
	return &fsDirStream{
		ctx:    ctx,
		dir:    n,
		lister: file.List(ctx, n.path, false /*nonrecursive*/),
	}, 0
}

func (n *inode) Unlink(_ context.Context, name string) syscall.Errno {
	childPath := file.Join(n.path, name)
	err := file.Remove(n.ctx(), childPath)
	log.Debug.Printf("unlink %s: err %v", childPath, err)
	return errToErrno(err)
}

func (n *inode) Rmdir(_ context.Context, name string) syscall.Errno {
	// Nothing to do.
	return 0
}

func (n *inode) Mkdir(ctx context.Context, name string, _ uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	n.mu.Lock()
	defer n.mu.Unlock()
	// TODO: Consider creating an S3 "directory" object so this new directory persists for new listings.
	// https://docs.aws.amazon.com/AmazonS3/latest/userguide/using-folders.html
	newPath := file.Join(n.path, name)
	childNode := &inode{
		path: newPath,
		ent: fuse.DirEntry{
			Name: name,
			Ino:  getIno(newPath),
			Mode: getModeBits(true)}}
	childInode := n.NewInode(ctx, childNode, fs.StableAttr{
		Mode: childNode.ent.Mode,
		Ino:  childNode.ent.Ino,
	})
	out.Attr = newAttr(n.ent.Ino, n.ent.Mode, 0, time.Time{})
	out.SetEntryTimeout(cacheExpiration)
	out.SetAttrTimeout(cacheExpiration)
	return childInode, 0
}
