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
	// dir entry of the parent directory. Used to produce
	// the fake ".." entry.
	parentEnt fuse.DirEntry

	mu   sync.Mutex // guards the following fields.
	stat cachedStat
	// Directory entries. Used only when this inode is a directory.
	dirCache *dirCache
}

// RootInode is a singleton inode created for the root mount point.
type rootInode struct {
	inode
	// The context to be used for all file operations. It's vcontext.Background()
	// in Grail environments.
	ctx context.Context
	// Directory for storing tmp files.
	tmpDir string
}

// Handle represents an open file handle.
type handle struct {
	// The file that the handle belongs to
	inode *inode
	// The parent of this file. dirInode.dirCache is updated if the handle is
	// opened for writing and it is closed successfully.
	dirInode *inode
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
func newHandle(inode, dirInode *inode, openMode uint32) *handle {
	return &handle{inode: inode, dirInode: dirInode, openMode: openMode, requestedSize: -1}
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
	expiration timestamp
	size       int64
	modTime    time.Time
}

type dirEntry struct {
	fuse.DirEntry
	stat cachedStat
}

// DirCache caches the whole contents of a directory.
type dirCache struct {
	expiration timestamp
	ents       []*dirEntry          // preserves the order of file.List()
	entMap     map[string]*dirEntry // maps filename -> element in ent[]
}

func downCast(n *fs.Inode) *inode {
	nn := (*inode)(unsafe.Pointer(n))
	if nn.path == "" {
		log.Panicf("not an inode: %+v", n)
	}
	return nn
}

var _ = (fs.InodeEmbedder)((*inode)(nil))
var _ = (fs.NodeAccesser)((*inode)(nil))
var _ = (fs.NodeCreater)((*inode)(nil))
var _ = (fs.NodeGetattrer)((*inode)(nil))
var _ = (fs.NodeSetattrer)((*inode)(nil))
var _ = (fs.NodeLookuper)((*inode)(nil))
var _ = (fs.NodeOpener)((*inode)(nil))
var _ = (fs.NodeReaddirer)((*inode)(nil))
var _ = (fs.NodeUnlinker)((*inode)(nil))
var _ = (fs.NodeRmdirer)((*inode)(nil))
var _ = (fs.NodeMkdirer)((*inode)(nil))

var _ = (fs.FileReader)((*handle)(nil))
var _ = (fs.FileWriter)((*handle)(nil))
var _ = (fs.FileFlusher)((*handle)(nil))
var _ = (fs.FileReleaser)((*handle)(nil))
var _ = (fs.FileFsyncer)((*handle)(nil))

// Remove an entry from the cache. Called on unlink.
//
// REQUIRES: inode.mu is locked.
func (c *dirCache) remove(name string) {
	old := c.entMap[name]
	if old == nil {
		return
	}
	for i := range c.ents {
		if c.ents[i] == old {
			n := len(c.ents)
			c.ents[i], c.ents[n-1] = c.ents[n-1], nil
			c.ents = c.ents[:n-1]
			delete(c.entMap, name)
			return
		}
	}
	log.Panicf("entry %v not found", name)
}

// Update adds or updates an entry. Called on create.
//
// REQUIRES: inode.mu is locked.
func (c *dirCache) update(e fuse.DirEntry, stat cachedStat) {
	if e.Ino == 0 || e.Name == "" || e.Mode == 0 {
		panic(e)
	}
	c.remove(e.Name)
	ent := &dirEntry{DirEntry: e, stat: stat}
	c.ents = append(c.ents, ent)
	c.entMap[e.Name] = ent
	log.Debug.Printf("dircache update: %+v stat %+v", e, ent.stat)
}

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
		return syscall.ECANCELED
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

// FsDirStream implements readdir.
type fsDirStream struct {
	dir *inode
	err error
	// the next entry to return. Indexes dir.dirCache.  next can be -1 or -2. -1
	// stands for ".", -2 for "..".
	next int
}

var _ = (fs.DirStream)((*fsDirStream)(nil))

// HasNext implements fs.DirStream
func (s *fsDirStream) HasNext() bool {
	s.dir.mu.Lock()
	defer s.dir.mu.Unlock()
	if s.err != nil {
		return false
	}
	dirCache, err := s.dir.listDirLocked()
	if err != nil {
		s.err = err
		return false
	}
	return s.next < len(dirCache.ents)
}

// Next implements fs.DirStream
func (s *fsDirStream) Next() (fuse.DirEntry, syscall.Errno) {
	s.dir.mu.Lock()
	defer s.dir.mu.Unlock()

	if s.err != nil {
		return fuse.DirEntry{}, errToErrno(s.err)
	}
	seq := s.next
	s.next++

	if seq == -2 {
		ent := s.dir.parentEnt
		ent.Name = ".."
		return ent, 0
	}
	if seq == -1 {
		ent := s.dir.ent
		ent.Name = "."
		return ent, 0
	}
	ent := s.dir.dirCache.ents[seq]
	return ent.DirEntry, 0
}

// Close implements fs.DirStream
func (s *fsDirStream) Close() {}

// Root reports the inode of the root mountpoint.
func (n *inode) root() *rootInode { return n.Root().Operations().(*rootInode) }

// Ctx reports the context passed from the application when mounting the
// filesystem.
func (n *inode) ctx() context.Context { return n.root().ctx }

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
	var (
		err  error
		stat file.Info
	)
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
	now := now()
	log.Debug.Printf("getattr %s: cached stats %+v now %+v", n.path, n.stat, now)
	if now.After(n.stat.expiration) {
		log.Debug.Printf("getattr %s: reading stats", n.path)
		stat, err = file.Stat(ctx, n.path)
		if err != nil {
			log.Printf("getattr %s: err %v", n.path, err)
			return errToErrno(err)
		}
		n.stat = cachedStat{
			expiration: now.Add(cacheExpiration),
			size:       stat.Size(),
			modTime:    stat.ModTime(),
		}
		log.Debug.Printf("getattr %s: refreshed %v", n.path, out)
	}
	out.Attr = newAttr(n.ent.Ino, n.ent.Mode, uint64(n.stat.size), n.stat.modTime)
	log.Debug.Printf("getattr %s: out %+v", n.path, out)
	return 0
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
	if fh.inode == nil || fh.dirInode == nil {
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
	now := now()
	n.stat.expiration = now.Add(cacheExpiration)
	n.stat.size = inSize
	n.stat.modTime = now.time
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
	if d := fh.dw; d != nil {
		n := fh.inode
		// There's not much we can do, but returning ENOSYS breaks too many apps.
		now := now()
		n.stat.expiration = now.Add(cacheExpiration)
		n.stat.size = d.off
		n.stat.modTime = now.time

		stat := n.stat
		dirInode := fh.dirInode
		log.Debug.Printf("fsync %s: update stats: stat=%v", n.path, n.stat)
		n.mu.Unlock()

		dirInode.mu.Lock()
		if dirInode.dirCache != nil {
			log.Debug.Printf("dircache update %s: %v %v", dirInode.path, n.ent, stat)
			dirInode.dirCache.update(n.ent, stat)
		}
		defer dirInode.mu.Unlock()
		return 0
	}
	n.mu.Unlock()
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

		now := now()
		n.stat.expiration = now.Add(cacheExpiration)
		n.stat.size = nByte
		n.stat.modTime = now.time

		stat := n.stat
		dirInode := fh.dirInode
		closeErrno := fh.closeErrno
		mu.Unlock()
		mu = nil
		if dirInode != nil && closeErrno == 0 {
			dirInode.mu.Lock()
			defer dirInode.mu.Unlock()
			if dirInode.dirCache != nil {
				dirInode.dirCache.update(n.ent, stat)
			}
		}
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
			now := now()
			n.stat.expiration = now.Add(cacheExpiration)
			n.stat.size = d.off
			n.stat.modTime = now.time
		}
		d.fp = nil
		d.w = nil
		stat := n.stat
		dirInode := fh.dirInode
		closeErrno := fh.closeErrno
		mu.Unlock()
		mu = nil
		if dirInode != nil && closeErrno == 0 {
			dirInode.mu.Lock()
			defer dirInode.mu.Unlock()
			if dirInode.dirCache != nil {
				dirInode.dirCache.update(n.ent, stat)
			}
		}
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
		path:      newPath,
		parentEnt: n.ent,
		ent: fuse.DirEntry{
			Name: name,
			Ino:  getIno(newPath),
			Mode: getModeBits(false)}}
	childInode := n.NewInode(ctx, childNode, fs.StableAttr{
		Mode: childNode.ent.Mode,
		Ino:  childNode.ent.Ino,
	})
	fh := newHandle(childNode, n, syscall.O_WRONLY|syscall.O_CREAT|syscall.O_TRUNC)
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
		fh := newHandle(n, nil, mode)
		fh.dr = &directRead{fp: fp, r: fp.Reader(ctx)}
		log.Debug.Printf("open %s: mode %x, fh %p", n.path, mode, fh)
		return fh, 0, 0
	}

	fh := newHandle(n, downCast(dirInode), mode)
	return fh, 0, 0
}

// listDirLocked reports the contents of a directory.
//
// REQUIRES: n.IsDir() && n.mu is locked
func (n *inode) listDirLocked() (*dirCache, error) {
	now := now()
	if n.dirCache != nil && !now.After(n.dirCache.expiration) {
		return n.dirCache, nil
	}
	log.Debug.Printf("listdir %s: start", n.path)

	lister := file.List(n.ctx(), n.path, false /*nonrecursive*/)
	var (
		dirNames = map[string]struct{}{}
		ents     []*dirEntry
	)
	for lister.Scan() {
		fileName := getFileName(n, lister.Path())
		if fileName == "" {
			// Assume this is a directory marker:
			// https://web.archive.org/web/20190424231712/https://docs.aws.amazon.com/AmazonS3/latest/user-guide/using-folders.html
			// s3file's List returns these, but empty filenames seem to cause problems for FUSE.
			// TODO: Filtering these in s3file, if it's ok for other users.
			continue
		}
		ent := &dirEntry{
			DirEntry: fuse.DirEntry{
				Name: fileName,
				Mode: getModeBits(lister.IsDir()),
				Ino:  getIno(lister.Path())},
		}
		if info := lister.Info(); info != nil {
			ent.stat.expiration = now.Add(cacheExpiration)
			ent.stat.size = info.Size()
			ent.stat.modTime = info.ModTime()
		}
		if lister.IsDir() {
			dirNames[fileName] = struct{}{}
		}
		ents = append(ents, ent)
	}
	if err := lister.Err(); err != nil {
		return nil, err
	}
	// Clear the existing contents
	if n.dirCache == nil {
		n.dirCache = &dirCache{entMap: map[string]*dirEntry{}}
	} else {
		for filename := range n.dirCache.entMap {
			delete(n.dirCache.entMap, filename)
		}
		n.dirCache.ents = n.dirCache.ents[:0]
	}
	// Add the contents to n.dirCache
	n.dirCache.expiration = now.Add(cacheExpiration)
	for _, ent := range ents {
		add := false
		if ent.Mode&syscall.S_IFDIR != 0 { // directory names are always unique
			add = true
		} else if _, ok := dirNames[ent.Name]; !ok {
			// For a regular file, we report only if there's no directory with the
			// same name. S3 doesn't prevent such situation from happening. In
			// particular, the "Make folder" feature in web console creates a
			// duplicate regular file.
			add = true
		}
		if add {
			n.dirCache.ents = append(n.dirCache.ents, ent)
			n.dirCache.entMap[ent.Name] = ent
		}
	}
	return n.dirCache, nil
}

func (n *inode) Lookup(_ context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	log.Debug.Printf("lookup %s: name=%s start", n.path, name)
	ctx := n.ctx()

	n.mu.Lock()
	defer n.mu.Unlock()
	dirCache, err := n.listDirLocked()
	if err != nil {
		return nil, errToErrno(err)
	}

	ent, ok := dirCache.entMap[name]
	if !ok {
		return nil, syscall.ENOENT
	}
	childNode := &inode{
		path:      file.Join(n.path, name),
		parentEnt: n.ent,
		ent:       ent.DirEntry,
		stat:      ent.stat}
	child := n.NewInode(ctx, childNode, fs.StableAttr{
		Mode: ent.Mode,
		Ino:  ent.Ino,
	})
	out.Attr = newAttr(childNode.ent.Ino, childNode.ent.Mode, uint64(childNode.stat.size), childNode.stat.modTime)
	log.Debug.Printf("lookup %s name=%s' done: mode=%o ino=%d stat=%+v", n.path, name, ent.Mode, ent.Ino, ent.stat)
	return child, 0
}

func (n *inode) Readdir(_ context.Context) (fs.DirStream, syscall.Errno) {
	log.Debug.Printf("readdir %s: start", n.path)
	return &fsDirStream{dir: n, next: -2 /*synthesize ".." and "."*/}, 0
}

func (n *inode) Unlink(_ context.Context, name string) syscall.Errno {
	childPath := file.Join(n.path, name)
	err := file.Remove(n.ctx(), childPath)
	log.Debug.Printf("unlink %s: err %v", childPath, err)
	if err == nil {
		n.mu.Lock()
		defer n.mu.Unlock()
		if n.dirCache != nil {
			n.dirCache.remove(name)
		}
	}
	return errToErrno(err)
}

func (n *inode) Rmdir(_ context.Context, name string) syscall.Errno {
	// Nothing to do.
	return 0
}

func (n *inode) Mkdir(ctx context.Context, name string, _ uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	n.mu.Lock()
	defer n.mu.Unlock()
	dirCache, err := n.listDirLocked()
	if err != nil {
		return nil, errToErrno(err)
	}
	newPath := file.Join(n.path, name)
	childNode := &inode{
		path:      newPath,
		parentEnt: n.ent,
		ent: fuse.DirEntry{
			Name: name,
			Ino:  getIno(newPath),
			Mode: getModeBits(true)}}
	childInode := n.NewInode(ctx, childNode, fs.StableAttr{
		Mode: childNode.ent.Mode,
		Ino:  childNode.ent.Ino,
	})
	out.Attr = newAttr(n.ent.Ino, n.ent.Mode, 0, time.Time{})
	dirCache.update(childNode.ent, cachedStat{})
	return childInode, 0
}
