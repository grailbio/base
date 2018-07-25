package fsnodefuse

import (
	"os"
	"sync/atomic"
	"testing"

	"github.com/grailbio/base/file/fsnode"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNameCollision verifies that we can list a directory in which there are
// entries with duplicate names without panicking.
func TestNameCollision(t *testing.T) {
	makeTest := func(children ...fsnode.T) func(*testing.T) {
		return func(t *testing.T) {
			root := fsnode.NewParent(
				fsnode.NewDirInfo("test"),
				fsnode.ConstChildren(children...),
			)
			withMounted(t, root, func(mountDir string) {
				f, err := os.Open(mountDir)
				require.NoError(t, err, "opening mounted directory")
				defer func() { assert.NoError(t, f.Close()) }()
				_, err = f.Readdir(0)
				assert.NoError(t, err, "reading mounted directory")
				assert.Zero(t, atomic.LoadUint32(&numHandledPanics), "checking number of panics")
			})
		}
	}
	var (
		aReg = fsnode.ConstLeaf(fsnode.NewRegInfo("a"), []byte{})
		aDir = fsnode.NewParent(fsnode.NewDirInfo("a"), fsnode.ConstChildren())
		bReg = fsnode.ConstLeaf(fsnode.NewRegInfo("b"), []byte{})
		bDir = fsnode.NewParent(fsnode.NewDirInfo("b"), fsnode.ConstChildren())
	)
	t.Run("reg_first", makeTest(aReg, aDir))
	t.Run("dir_first", makeTest(aDir, aReg))
	t.Run("mixed", makeTest(aReg, aDir, aDir, bReg, aReg, bReg, aReg, bDir, aReg, aDir))
}
