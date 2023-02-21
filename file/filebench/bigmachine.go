package filebench

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"time"

	"github.com/grailbio/base/errors"
	"github.com/grailbio/base/must"
	"github.com/grailbio/base/traverse"
	"github.com/grailbio/bigmachine"
)

// Bigmachine configures a cluster of remote machines to each execute benchmarks and then report
// their results.
type Bigmachine struct {
	// Bs is the collection of bigmachines in which to run benchmarks. EC2 instance type is a
	// property of *bigmachine.B, so this lets callers benchmark several EC2 instance types.
	// (*B).Name() is printed to identify resutls. Caller remains responsible for shutdown.
	Bs       []*bigmachine.B
	Environ  bigmachine.Environ
	Services bigmachine.Services
}

// NewBigmachine returns a new configuration, ready for callers to configure. Callers likely want
// to add bigmachines for remote execution (otherwise it just falls back to local).
// Or, they may add environment variables or services for AWS credentials.
func NewBigmachine(rs ReadSizes) Bigmachine {
	return Bigmachine{
		Services: bigmachine.Services{
			"FileBench": benchService{rs},
		},
	}
}

// RunAndPrint starts a machine in each d.Bs and then executes ReadSizes.RunAndPrint on it.
// It writes all the machine results to out, identifying each section by d.Bs's keys.
func (d Bigmachine) RunAndPrint(
	ctx context.Context,
	out io.Writer,
	pathPrefixes []Prefix,
	pathSuffix0 string,
	pathSuffixes ...string,
) error {
	var results = make([]string, len(d.Bs))

	err := traverse.Each(len(d.Bs), func(bIdx int) error {
		b := d.Bs[bIdx]
		machines, err := b.Start(ctx, 1, d.Environ, d.Services)
		if err != nil {
			return err
		}
		machine := machines[0]

		// Benchmark runs have encountered some throttling from S3 (503 SlowDown). Sleep a bit
		// to separate out the benchmark runs, so that each benchmarking machine is likely to
		// locate some distinct S3 remote IPs. (Due to VPC DNS caching, if all the machines start
		// simultaneously, they're likely to use the same S3 peers.) Of course, this introduces
		// systematic bias in comparing results between machines, but we accept that for now.
		time.Sleep(time.Minute * time.Duration(bIdx))

		return machine.Call(ctx, "FileBench.Run",
			benchRequest{pathPrefixes, pathSuffix0, pathSuffixes},
			&results[bIdx])
	})

	for bIdx, result := range results {
		if result == "" {
			continue
		}
		if bIdx > 0 {
			_, err := fmt.Fprintln(out)
			must.Nil(err)
		}
		_, err := fmt.Fprintf(out, "[%d] %s\n%s", bIdx, d.Bs[bIdx].Name(), result)
		must.Nil(err)
	}
	return err
}

type (
	benchService struct{ ReadSizes }
	benchRequest struct {
		PathPrefixes []Prefix
		PathSuffix0  string
		PathSuffixes []string
	}

	fuseService struct{}
)

func init() {
	gob.Register(benchService{})
	gob.Register(fuseService{})
}

func (s benchService) Run(ctx context.Context, req benchRequest, out *string) error {
	var buf bytes.Buffer
	s.ReadSizes.RunAndPrint(ctx, &buf, req.PathPrefixes, req.PathSuffix0, req.PathSuffixes...)
	*out = buf.String()
	return nil
}

// AddS3FUSE configures d so that each machine running benchmarks can access S3 objects through
// the local filesystem, at mountPath. For example, object s3://b/my/key will appear at
// $mountPath/b/my/key. Callers can use this to construct paths for RunAndPrint.
func (d Bigmachine) AddS3FUSE() (mountPath string) {
	must.True(len(s3FUSEBinary) > 0)
	d.Services["s3FUSE"] = fuseService{}
	return s3FUSEPath
}

const s3FUSEPath = "/tmp/s3"

func (fuseService) Init(*bigmachine.B) (err error) {
	defer func() {
		if err != nil {
			err = errors.E(err, errors.Fatal)
		}
	}()
	if err := os.MkdirAll(s3FUSEPath, 0700); err != nil {
		return err
	}
	ents, err := os.ReadDir(s3FUSEPath)
	if err != nil {
		return err
	}
	if len(ents) > 0 {
		return errors.New("s3 fuse mount is non-empty")
	}
	tmpDir, err := os.MkdirTemp("", "s3fuse-*")
	if err != nil {
		return err
	}
	exe := path.Join(tmpDir, "s3fuse")
	if err := os.WriteFile(exe, s3FUSEBinary, 0700); err != nil {
		return err
	}
	cmdErrC := make(chan error)
	go func() {
		out, err := exec.Command(exe, s3FUSEPath).CombinedOutput()
		if err == nil {
			err = errors.E("s3fuse exited unexpectedly")
		}
		cmdErrC <- errors.E(err, fmt.Sprintf("s3fuse output:\n%s", out))
	}()
	readDirC := make(chan error)
	go func() {
		for {
			ents, err := os.ReadDir(s3FUSEPath)
			if err != nil {
				readDirC <- err
				return
			}
			if len(ents) > 0 {
				readDirC <- nil
			}
			time.Sleep(time.Second)
		}
	}()
	select {
	case err = <-cmdErrC:
	case err = <-readDirC:
	case <-time.After(10 * time.Second):
		err = errors.New("ran out of time waiting for FUSE mount")
	}
	return err
}
