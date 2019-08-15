# Grail-fuse

Grail-fuse allows reading and writing S3 files as if they are on the local file
system.  It supports Linux and OSX. No extra step is needed for Linux. For OSX,
install osxfuse from https://github.com/osxfuse/osxfuse/releases

## Usage

    grail-fuse [-remote-root-dir DIR] [-daemon] [-log-dir LOGDIR] MOUNTPOINT

Example:

    grail-fuse -daemon $HOME/s3

Flag `-mount-root-dir` defaults to `s3://`.  Thus, after mounting grail-fuse on
`~/s3`, Toplevel directories under `~/s3` will list S3 buckets owned by the
default AWS account. You can descend into buckets to read files underneath.

If `-daemon` is set, `grail-fuse` runs as a daemon.

## Unmounting the file system

To unmount `grail-fuse`, run:

    fusermount -u $HOME/s3

on Linux, or

    umount $HOME/s3

on OSX.

## Bugs and limitations

- `mkdir` is not supported. `rmdir` is supported, but is is just a noop.  You can
  just create a file under a nonexisting subdirectory without `mkdir`ing it first.

- grail-fuse caches file attributes in memory for up to 5 minutes. Thus, if the
  remote file system is updated by another user, it will not be reflected for up
  to five minutes.  You can send a SIGHUP to `grail-fuse` to invalidate the
  cache.

- File contents are not written back to the remote file system until `close`.

- In general, random seeks are supported during writes.
