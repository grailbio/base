//+build !unit

package gfs_test

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	golog "log"
	"os"
	"sort"
	"testing"

	"os/exec"
	"syscall"

	"github.com/grailbio/base/cmd/grail-fuse/gfs"
	"github.com/grailbio/base/log"
	"github.com/grailbio/testutil/assert"
	"github.com/grailbio/testutil/expect"
	"github.com/grailbio/testutil/h"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

type tester struct {
	t         *testing.T
	remoteDir string
	mountDir  string
	tempDir   string
	server    *fuse.Server
}

type logOutputter struct{}

func (logOutputter) Level() log.Level { return log.Debug }

func (logOutputter) Output(calldepth int, level log.Level, s string) error {
	return golog.Output(calldepth+1, s)
}

func newTester(t *testing.T, remoteDir string) *tester {
	log.SetFlags(log.Lmicroseconds | log.Lshortfile)
	log.SetOutputter(logOutputter{})
	if remoteDir == "" {
		var err error
		remoteDir, err = ioutil.TempDir("", "remote")
		assert.NoError(t, err)
	}
	tempDir, err := ioutil.TempDir("", "temp")
	assert.NoError(t, err)
	mountDir, err := ioutil.TempDir("", "mount")
	assert.NoError(t, err)
	root := gfs.NewRoot(context.Background(), remoteDir, tempDir)
	server, err := fs.Mount(mountDir, root, &fs.Options{
		MountOptions: fuse.MountOptions{
			FsName:        "test",
			DisableXAttrs: true,
			Debug:         true}})
	assert.NoError(t, err)
	log.Printf("mount remote dir %s on %s, tmp %s", remoteDir, mountDir, tempDir)
	return &tester{
		t:         t,
		remoteDir: remoteDir,
		mountDir:  mountDir,
		tempDir:   tempDir,
		server:    server,
	}
}

func (t *tester) MountDir() string  { return t.mountDir }
func (t *tester) RemoteDir() string { return t.remoteDir }

func (t *tester) Cleanup() {
	log.Printf("unmount %s", t.mountDir)
	assert.NoError(t.t, t.server.Unmount())
	assert.NoError(t.t, os.RemoveAll(t.mountDir))
	log.Printf("unmount %s done", t.mountDir)
}

func writeFile(t *testing.T, path string, data string) {
	assert.NoError(t, ioutil.WriteFile(path, []byte(data), 0600))
}

func readFile(path string) string {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return fmt.Sprintf("read %s: error %v", path, err)
	}
	return string(data)
}

func readdir(t *testing.T, dir string) []string {
	fp, err := os.Open(dir)
	assert.NoError(t, err)
	names, err := fp.Readdirnames(0)
	assert.NoError(t, err)
	sort.Strings(names)
	assert.NoError(t, fp.Close())
	return names
}

func TestSimple(t *testing.T) {
	var (
		err       error
		remoteDir string
	)
	if remoteDir, err = ioutil.TempDir("", "remote"); err != nil {
		log.Panic(err)
	}
	defer func() { _ = os.RemoveAll(remoteDir) }()

	writeFile(t, remoteDir+"/fox.txt", "pink fox")
	tc := newTester(t, remoteDir)
	defer tc.Cleanup()

	expect.EQ(t, readFile(tc.MountDir()+"/fox.txt"), "pink fox")
	expect.That(t, readdir(t, tc.MountDir()), h.ElementsAre("fox.txt"))
	assert.NoError(t, os.Remove(tc.MountDir()+"/fox.txt"))
	expect.HasSubstr(t, readFile(tc.MountDir()+"/fox.txt"), "no such file")
	expect.HasSubstr(t, readFile(remoteDir+"/fox.txt"), "no such file")
	expect.That(t, readdir(t, tc.MountDir()), h.ElementsAre())
}

func TestOverwrite(t *testing.T) {
	tc := newTester(t, "")
	defer tc.Cleanup()

	path := tc.MountDir() + "/bar.txt"
	fp, err := os.Create(path)
	assert.NoError(t, err)
	_, err = fp.Write([]byte("purple dog"))
	assert.NoError(t, err)
	assert.NoError(t, fp.Close())
	expect.EQ(t, readFile(tc.RemoteDir()+"/bar.txt"), "purple dog")
	expect.EQ(t, readFile(path), "purple dog")

	fp, err = os.Create(path)
	assert.NoError(t, err)
	_, err = fp.Write([]byte("white giraffe"))
	assert.NoError(t, err)
	assert.NoError(t, fp.Close())
	expect.EQ(t, readFile(tc.RemoteDir()+"/bar.txt"), "white giraffe")
	expect.EQ(t, readFile(path), "white giraffe")
}

func TestReadWrite(t *testing.T) {
	tc := newTester(t, "")
	defer tc.Cleanup()

	path := tc.MountDir() + "/baz.txt"
	writeFile(t, path, "purple cat")
	fp, err := os.OpenFile(path, os.O_RDWR, 0600)
	assert.NoError(t, err)
	_, err = fp.Write([]byte("yellow"))
	assert.NoError(t, err)
	assert.NoError(t, fp.Close())
	expect.EQ(t, readFile(path), "yellow cat")

	fp, err = os.OpenFile(path, os.O_RDWR, 0600)
	assert.NoError(t, err)
	_, err = fp.Seek(7, io.SeekStart)
	assert.NoError(t, err)
	_, err = fp.Write([]byte("bat"))
	assert.NoError(t, fp.Close())
	expect.EQ(t, readFile(path), "yellow bat")
}

func TestAppend(t *testing.T) {
	tc := newTester(t, "")
	defer tc.Cleanup()

	path := tc.MountDir() + "/append.txt"
	writeFile(t, path, "orange ape")
	log.Printf("reopening %s with O_APPEND", path)
	fp, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0600)
	assert.NoError(t, err)
	log.Printf("writing to %s", path)
	_, err = fp.Write([]byte("red donkey"))
	assert.NoError(t, err)
	assert.NoError(t, fp.Close())
	expect.EQ(t, readFile(path), "orange apered donkey")
}

func TestMkdir(t *testing.T) {
	tc := newTester(t, "")
	defer tc.Cleanup()

	path := tc.MountDir() + "/dir0"
	assert.NoError(t, os.Mkdir(path, 0755))
	assert.EQ(t, readdir(t, path), []string{})
}

func TestDup(t *testing.T) {
	tc := newTester(t, "")
	defer tc.Cleanup()

	path := tc.MountDir() + "/f0.txt"
	fp0, err := os.Create(path)
	assert.NoError(t, err)

	fd1, err := syscall.Dup(int(fp0.Fd()))
	assert.NoError(t, err)
	fp1 := os.NewFile(uintptr(fd1), path)

	_, err = fp0.Write([]byte("yellow bug"))
	assert.NoError(t, err)
	assert.NoError(t, fp0.Close())
	_, err = fp1.Write([]byte("yellow hug"))
	assert.HasSubstr(t, err, "bad file descriptor")
	assert.NoError(t, fp1.Close())
	expect.EQ(t, readFile(path), "yellow bug")
}

func TestShell(t *testing.T) {
	tc := newTester(t, "")
	defer tc.Cleanup()

	path := tc.MountDir() + "/cat.txt"
	cmd := exec.Command("sh", "-c", fmt.Sprintf("echo foo >%s", path))
	assert.NoError(t, cmd.Run())
	expect.EQ(t, readFile(path), "foo\n")

	cmd = exec.Command("sh", "-c", fmt.Sprintf("echo bar >>%s", path))
	assert.NoError(t, cmd.Run())
	expect.EQ(t, readFile(path), "foo\nbar\n")

	path2 := tc.MountDir() + "/cat2.txt"
	log.Printf("Start cat")
	cmd = exec.Command("sh", "-c", fmt.Sprintf("cat <%s >%s", path, path2))
	assert.NoError(t, cmd.Run())
	expect.EQ(t, readFile(path2), "foo\nbar\n")
}
