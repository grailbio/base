package fsnode

import (
	"os"
	"time"
)

// FileInfo implements os.FileInfo. Instances are immutable but convenient copy-and-set methods
// are provided for some fields.
type FileInfo struct {
	name         string
	size         int64
	mode         os.FileMode
	mod          time.Time
	sys          interface{}
	cacheableFor time.Duration
}

// NewDirInfo constructs FileInfo for a directory.
// Default ModePerm is 0555 (r-xr-xr-x). Other defaults are zero.
func NewDirInfo(name string) FileInfo { return FileInfo{name: name, mode: os.ModeDir | 0555} }

// NewRegInfo constructs FileInfo for a regular file.
// Default ModePerm is 0444 (r--r--r--). Other defaults are zero.
func NewRegInfo(name string) FileInfo { return FileInfo{name: name, mode: 0444} }

// CopyFileInfo constructs FileInfo with the same public fields as info.
// It copies cacheability if available.
func CopyFileInfo(info os.FileInfo) FileInfo {
	return FileInfo{
		name:         info.Name(),
		size:         info.Size(),
		mode:         info.Mode(),
		mod:          info.ModTime(),
		sys:          info.Sys(),
		cacheableFor: CacheableFor(info),
	}
}

func (f FileInfo) Name() string                { return f.name }
func (f FileInfo) Size() int64                 { return f.size }
func (f FileInfo) Mode() os.FileMode           { return f.mode }
func (f FileInfo) ModTime() time.Time          { return f.mod }
func (f FileInfo) IsDir() bool                 { return f.mode&os.ModeDir != 0 }
func (f FileInfo) Sys() interface{}            { return f.sys }
func (f FileInfo) CacheableFor() time.Duration { return f.cacheableFor }

func (f FileInfo) WithName(name string) FileInfo {
	cp := f
	cp.name = name
	return cp
}
func (f FileInfo) WithSize(size int64) FileInfo {
	cp := f
	cp.size = size
	return cp
}
func (f FileInfo) WithModePerm(perm os.FileMode) FileInfo {
	cp := f
	cp.mode = (perm & os.ModePerm) | (cp.mode &^ os.ModePerm)
	return cp
}
func (f FileInfo) WithModeType(modeType os.FileMode) FileInfo {
	cp := f
	cp.mode = (modeType & os.ModeType) | (cp.mode &^ os.ModeType)
	return cp
}
func (f FileInfo) WithModTime(mod time.Time) FileInfo {
	cp := f
	cp.mod = mod
	return cp
}
func (f FileInfo) WithSys(sys interface{}) FileInfo {
	cp := f
	cp.sys = sys
	return cp
}
func (f FileInfo) WithCacheableFor(d time.Duration) FileInfo {
	cp := f
	cp.cacheableFor = d
	return cp
}

func (f FileInfo) Equal(g FileInfo) bool {
	if !f.mod.Equal(g.mod) {
		return false
	}
	f.mod = g.mod
	return f == g
}
