package testutil

import (
	"fmt"
	"os/exec"
	"strings"
	"testing"

	assert "github.com/stretchr/testify/assert"

	v23 "v.io/v23"
	v23context "v.io/v23/context"
	"v.io/v23/naming"
	"v.io/v23/rpc"
	"v.io/v23/security"
)

// RunBlesserServer runs a test v23 server, returns a context and an endpoint
func RunBlesserServer(ctx *v23context.T, t *testing.T, stub interface{}) (*v23context.T, naming.Endpoint) {
	ctx = v23.WithListenSpec(ctx, rpc.ListenSpec{
		Addrs: rpc.ListenAddrs{{"tcp", "localhost:0"}},
	})
	ctx, blesserServer, err := v23.WithNewServer(ctx, "", stub, security.AllowEveryone())
	assert.NoError(t, err)
	blesserEndpoints := blesserServer.Status().Endpoints
	assert.Equal(t, 1, len(blesserEndpoints))
	return ctx, blesserEndpoints[0]
}

// RunAndCapture runs a command and captures the stdout and stderr
func RunAndCapture(t *testing.T, cmd *exec.Cmd) (stdout, stderr string) {
	var stdoutBuf, stderrBuf strings.Builder
	cmd.Stdout = &stdoutBuf
	cmd.Stderr = &stderrBuf
	err := cmd.Run()
	stdout, stderr = stdoutBuf.String(), stderrBuf.String()
	assert.NoError(t, err, fmt.Sprintf("stdout: '%s', stderr: '%s'", stdout, stderr))
	return
}
