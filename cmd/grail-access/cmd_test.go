package main_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/grailbio/base/security/identity"
	"github.com/grailbio/testutil"
	"github.com/stretchr/testify/assert"
	v23 "v.io/v23"
	"v.io/v23/context"
	"v.io/v23/naming"
	"v.io/v23/rpc"
	"v.io/v23/security"
	libsecurity "v.io/x/ref/lib/security"
)

func TestCmd(t *testing.T) {
	exe := testutil.GoExecutable(t, "//go/src/github.com/grailbio/base/cmd/grail-access/grail-access")

	// Preserve the test environment's PATH. On Darwin, Vanadium's agentlib uses `ioreg` from the
	// path in the process of locking [1] and loading [2] the principal when there's no agent.
	// [1] https://github.com/vanadium/core/blob/694a147f5dfd7ebc2d2e5a4fb3c4fe448c7a377c/x/ref/services/agent/internal/lockutil/version1_darwin.go#L21
	// [2] https://github.com/vanadium/core/blob/694a147f5dfd7ebc2d2e5a4fb3c4fe448c7a377c/x/ref/services/agent/agentlib/principal.go#L57
	pathEnv := "PATH=" + os.Getenv("PATH")

	t.Run("help", func(t *testing.T) {
		cmd := exec.Command(exe, "-help")
		cmd.Env = []string{pathEnv}
		stdout, stderr := runAndCapture(t, cmd)
		assert.NotEmpty(t, stdout)
		assert.Empty(t, stderr)
	})

	// TODO(josh): Test with v23agentd on the path, too.
	t.Run("dump_existing_principal", func(t *testing.T) {
		homeDir, cleanUp := testutil.TempDir(t, "", "")
		defer cleanUp()
		principalDir := path.Join(homeDir, ".v23")
		decoyPrincipalDir := path.Join(homeDir, "decoy_principal_dir")
		principal, err := libsecurity.CreatePersistentPrincipal(principalDir, nil)
		assert.NoError(t, err)

		const blessingName = "grail-access-test-blessing-ln7z94"
		blessings, err := principal.BlessSelf(blessingName)
		assert.NoError(t, err)
		assert.NoError(t, principal.BlessingStore().SetDefault(blessings))

		t.Run("flag_dir", func(t *testing.T) {
			cmd := exec.Command(exe, "-dump", "-dir", principalDir)
			// Set $V23_CREDENTIALS to test that -dir takes priority.
			cmd.Env = []string{pathEnv, "V23_CREDENTIALS=" + decoyPrincipalDir}
			stdout, stderr := runAndCapture(t, cmd)
			assert.Contains(t, stdout, blessingName)
			assert.Empty(t, stderr)
		})

		t.Run("env_home", func(t *testing.T) {
			cmd := exec.Command(exe, "-dump")
			cmd.Env = []string{pathEnv, "HOME=" + homeDir}
			stdout, stderr := runAndCapture(t, cmd)
			assert.Contains(t, stdout, blessingName)
			assert.Empty(t, stderr)
		})
	})

	t.Run("do_not_refresh/existing", func(t *testing.T) {
		principalDir, cleanUp := testutil.TempDir(t, "", "")
		defer cleanUp()
		principal, err := libsecurity.CreatePersistentPrincipal(principalDir, nil)
		assert.NoError(t, err)

		const blessingName = "grail-access-test-blessing-nuz823"
		doNotRefreshDuration := time.Hour
		expirationTime := time.Now().Add(2 * doNotRefreshDuration)
		expiryCaveat, err := security.NewExpiryCaveat(expirationTime)
		assert.NoError(t, err)
		blessings, err := principal.BlessSelf(blessingName, expiryCaveat)
		assert.NoError(t, err)
		assert.NoError(t, principal.BlessingStore().SetDefault(blessings))

		cmd := exec.Command(exe,
			"-dir", principalDir,
			"-do-not-refresh-duration", doNotRefreshDuration.String())
		cmd.Env = []string{pathEnv}
		stdout, stderr := runAndCapture(t, cmd)
		assert.Contains(t, stdout, blessingName)
		assert.Empty(t, stderr)
	})

	t.Run("fake_v23_servers", func(t *testing.T) {
		ctx, v23CleanUp := v23.Init()
		defer v23CleanUp()

		t.Run("ec2", func(t *testing.T) {
			const (
				wantDoc                 = "grailaccesstesttoken92lsl83"
				serverBlessingName      = "grail-access-test-blessing-laul37"
				clientBlessingExtension = "ec2-test"
				wantClientBlessing      = serverBlessingName + ":" + clientBlessingExtension
			)

			// Run fake ticket server: accepts EC2 instance identity document, returns blessings.
			var blesserEndpoint naming.Endpoint
			ctx, blesserEndpoint = runBlesserServer(ctx, t,
				identity.Ec2BlesserServer(fakeBlesser(
					func(gotDoc string, recipient security.PublicKey) security.Blessings {
						assert.Equal(t, wantDoc, gotDoc)
						p := v23.GetPrincipal(ctx)
						caveat, err := security.NewExpiryCaveat(time.Now().Add(24 * time.Hour))
						assert.NoError(t, err)
						localBlessings, err := p.BlessSelf(serverBlessingName)
						assert.NoError(t, err)
						b, err := p.Bless(recipient, localBlessings, clientBlessingExtension, caveat)
						assert.NoError(t, err)
						return b
					}),
				),
			)

			// Run fake EC2 instance identity server.
			listener, err := net.Listen("tcp", "localhost:")
			assert.NoError(t, err)
			defer func() { assert.NoError(t, listener.Close()) }()
			go http.Serve( // nolint: errcheck
				listener,
				http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
					_, httpErr := w.Write([]byte(wantDoc))
					assert.NoError(t, httpErr)
				}),
			)

			// Run grail-access to create a principal and bless it with the EC2 flow.
			principalDir, principalCleanUp := testutil.TempDir(t, "", "")
			defer principalCleanUp()
			cmd := exec.Command(exe,
				"-dir", principalDir,
				"-ec2",
				"-blesser-ec2", fmt.Sprintf("/%s", blesserEndpoint.Address),
				"-ec2-instance-identity-url", fmt.Sprintf("http://%s/", listener.Addr().String()))
			cmd.Env = []string{pathEnv}
			stdout, _ := runAndCapture(t, cmd)
			assert.Contains(t, stdout, wantClientBlessing)

			// Make sure we got the right blessing.
			principal, err := libsecurity.LoadPersistentPrincipal(principalDir, nil)
			assert.NoError(t, err)
			defaultBlessing, _ := principal.BlessingStore().Default()
			assert.Contains(t, defaultBlessing.String(), wantClientBlessing)
		})

		t.Run("google", func(t *testing.T) {
			const (
				wantToken               = "grailaccesstesttokensjo289d"
				serverBlessingName      = "grail-access-test-blessing-s8j9dk"
				clientBlessingExtension = "google-test"
				wantClientBlessing      = serverBlessingName + ":" + clientBlessingExtension
			)

			// Run fake ticket server: accepts Google ID token, returns blessings.
			var blesserEndpoint naming.Endpoint
			ctx, blesserEndpoint = runBlesserServer(ctx, t,
				identity.GoogleBlesserServer(fakeBlesser(
					func(gotToken string, recipient security.PublicKey) security.Blessings {
						assert.Equal(t, wantToken, gotToken)
						p := v23.GetPrincipal(ctx)
						caveat, err := security.NewExpiryCaveat(time.Now().Add(24 * time.Hour))
						assert.NoError(t, err)
						localBlessings, err := p.BlessSelf(serverBlessingName)
						assert.NoError(t, err)
						b, err := p.Bless(recipient, localBlessings, clientBlessingExtension, caveat)
						assert.NoError(t, err)
						return b
					}),
				),
			)

			// Run fake oauth server.
			listener, err := net.Listen("tcp", "localhost:")
			assert.NoError(t, err)
			defer func() { assert.NoError(t, listener.Close()) }()
			go http.Serve( // nolint: errcheck
				listener,
				http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
					if req.URL.Path != "/token" {
						assert.FailNowf(t, "fake oauth server: unexpected request: %s", req.URL.Path)
					}
					w.Header().Set("Content-Type", "application/json")
					assert.NoError(t, json.NewEncoder(w).Encode(
						map[string]interface{}{
							"access_token": "testtoken",
							"expires_in":   3600,
							"id_token":     wantToken,
							"scope":        "https://www.googleapis.com/auth/userinfo.email",
						},
					))
				}),
			)

			// Run grail-access to create a principal and bless it with the EC2 flow.
			principalDir, principalCleanUp := testutil.TempDir(t, "", "")
			defer principalCleanUp()
			cmd := exec.Command(exe,
				"-dir", principalDir,
				"-browser=false",
				"-blesser-google", fmt.Sprintf("/%s", blesserEndpoint.Address),
				"-google-oauth2-url", fmt.Sprintf("http://%s", listener.Addr().String()))
			cmd.Env = []string{pathEnv}
			cmd.Stdin = bytes.NewReader([]byte("testcode"))
			stdout, _ := runAndCapture(t, cmd)
			assert.Contains(t, stdout, wantClientBlessing)

			// Make sure we got the right blessing.
			principal, err := libsecurity.LoadPersistentPrincipal(principalDir, nil)
			assert.NoError(t, err)
			defaultBlessing, _ := principal.BlessingStore().Default()
			assert.Contains(t, defaultBlessing.String(), wantClientBlessing)
		})
	})
}

func runAndCapture(t *testing.T, cmd *exec.Cmd) (stdout, stderr string) {
	var stdoutBuf, stderrBuf strings.Builder
	cmd.Stdout = &stdoutBuf
	cmd.Stderr = &stderrBuf
	err := cmd.Run()
	stdout, stderr = stdoutBuf.String(), stderrBuf.String()
	assert.NoError(t, err, fmt.Sprintf("stdout: '%s', stderr: '%s'", stdout, stderr))
	return
}

type fakeBlesser func(arg string, recipientKey security.PublicKey) security.Blessings

func (f fakeBlesser) BlessEc2(_ *context.T, call rpc.ServerCall, s string) (security.Blessings, error) {
	return f(s, call.Security().RemoteBlessings().PublicKey()), nil
}

func (f fakeBlesser) BlessGoogle(_ *context.T, call rpc.ServerCall, s string) (security.Blessings, error) {
	return f(s, call.Security().RemoteBlessings().PublicKey()), nil
}

func runBlesserServer(ctx *context.T, t *testing.T, stub interface{}) (*context.T, naming.Endpoint) {
	ctx = v23.WithListenSpec(ctx, rpc.ListenSpec{
		Addrs: rpc.ListenAddrs{{"tcp", "localhost:0"}},
	})
	ctx, blesserServer, err := v23.WithNewServer(ctx, "", stub, security.AllowEveryone())
	assert.NoError(t, err)
	blesserEndpoints := blesserServer.Status().Endpoints
	assert.Equal(t, 1, len(blesserEndpoints))
	return ctx, blesserEndpoints[0]
}
