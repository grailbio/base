package gfs

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/grailbio/base/log"
	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

const daemonEnv = "_GFS_SLAVE"

func logSuffix() string {
	return time.Now().Format(time.RFC3339) + ".log"
}

// Main starts the FUSE server. It arranges so that contents of remoteRootDir
// can be accessed through mountDir. Arg remoteRootDir is typically
// "s3://". mountDir must be a directory in the local file system.
//
// If daemon==true, this function will fork itself to become a background
// process, and this process will exit. Otherwise this function blocks until the
// filesystem is unmounted by a superuser running "umount <dir>".
//
// Arg tmpDir specifies the directory to store temporary files. If tmpDir=="",
// it is set to /tmp/gfs-cache-<uid>.
//
// logDir specifies the directory for storing log files. If "", log messages are
// sent to stderr.
func Main(ctx context.Context, remoteRootDir, mountDir string, daemon bool, tmpDir, logDir string) {
	if daemon {
		daemonize(logDir)
		// daemonize will exit the parent process if daemon==true
	} else if logDir != "" {
		path := filepath.Join(logDir, "gfs."+logSuffix())
		fd, err := os.OpenFile(path, syscall.O_CREAT|syscall.O_WRONLY|syscall.O_APPEND, 0600)
		if err != nil {
			log.Panicf("create %s: %v", path, err)
		}
		log.Printf("Storing log files in %s", path)
		log.SetOutput(fd)
	}
	if err := os.MkdirAll(mountDir, 0700); err != nil {
		log.Panicf("mkdir %s: %v", mountDir, err)
	}
	root := NewRoot(ctx, remoteRootDir, tmpDir)
	server, err := fs.Mount(mountDir, root, &fs.Options{
		MountOptions: fuse.MountOptions{
			FsName:        "grail",
			DisableXAttrs: true,
			Debug:         log.At(log.Debug)}})
	if err != nil {
		log.Panicf("mount %s: %v", mountDir, err)
	}
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGHUP)
	go func() {
		for _ = range sigCh {
			log.Print("Received HUP signal")
			newEpoch()
		}
	}()
	server.Wait()
}

func daemonize(logDir string) {
	if os.Getenv(daemonEnv) == "" {
		suffix := logSuffix()
		if logDir == "" {
			logDir = "/tmp"
			log.Printf("Storing log files in %s/gfs-*%s", logDir, suffix)
		}
		if logDir == "" {
			log.Panic("-log-dir must set with -daemon")
		}
		stdinFd, err := os.Open("/dev/null")
		if err != nil {
			log.Panic(err)
		}
		stdoutFd, err := os.Create(filepath.Join(logDir, "gfs-stdout."+suffix))
		if err != nil {
			log.Panic(err)
		}
		stderrFd, err := os.Create(filepath.Join(logDir, "gfs-stderr."+suffix))
		if err != nil {
			log.Panic(err)
		}
		os.Stdout.Sync()
		os.Stderr.Sync()
		cmd := exec.Command(os.Args[0], os.Args[1:]...)
		cmd.Stdout = stdoutFd
		cmd.Stderr = stderrFd
		cmd.Stdin = stdinFd
		cmd.Env = append([]string(nil), os.Environ()...)
		cmd.Env = append(cmd.Env, daemonEnv+"=1")
		cmd.Start()
		os.Exit(0)
	}
}

func NewRoot(ctx context.Context, remoteRootDir, tmpDir string) fs.InodeEmbedder {
	if tmpDir == "" {
		tmpDir = fmt.Sprintf("/tmp/gfscache-%d", os.Geteuid())
	}
	if !strings.HasSuffix(remoteRootDir, "/") {
		// getFileName misbehaves otherwise.
		remoteRootDir += "/"
	}
	if err := os.MkdirAll(tmpDir, 0700); err != nil {
		log.Panic(err)
	}
	ent := fuse.DirEntry{
		Name: "/",
		Ino:  getIno(""),
		Mode: getModeBits(true)}
	return &rootInode{inode: inode{path: remoteRootDir, ent: ent, parentEnt: ent}, ctx: ctx, tmpDir: tmpDir}
}
