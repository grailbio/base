package main_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path"
	"strings"
	"testing"
	"time"

	ticketServerUtil "github.com/grailbio/base/cmd/ticket-server/testutil"
	"github.com/grailbio/base/security/identity"
	"github.com/grailbio/testutil"
	_ "github.com/grailbio/v23/factories/grail"
	"github.com/stretchr/testify/assert"
	v23 "v.io/v23"
	"v.io/v23/context"
	"v.io/v23/naming"
	"v.io/v23/rpc"
	"v.io/v23/security"
	"v.io/x/ref"
	libsecurity "v.io/x/ref/lib/security"
)

func TestCmd(t *testing.T) {
	ctx, v23CleanUp := v23.Init()
	defer v23CleanUp()
	assert.NoError(t, ref.EnvClearCredentials())
	exe := testutil.GoExecutable(t, "//go/src/github.com/grailbio/base/cmd/grail-access/grail-access")

	// Preserve the test environment's PATH. On Darwin, Vanadium's agentlib uses `ioreg` from the
	// path in the process of locking [1] and loading [2] the principal when there's no agent.
	// [1] https://github.com/vanadium/core/blob/694a147f5dfd7ebc2d2e5a4fb3c4fe448c7a377c/x/ref/services/agent/internal/lockutil/version1_darwin.go#L21
	// [2] https://github.com/vanadium/core/blob/694a147f5dfd7ebc2d2e5a4fb3c4fe448c7a377c/x/ref/services/agent/agentlib/principal.go#L57
	pathEnv := "PATH=" + os.Getenv("PATH")

	t.Run("help", func(t *testing.T) {
		cmd := exec.Command(exe, "-help")
		cmd.Env = []string{pathEnv}
		stdout, stderr := ticketServerUtil.RunAndCapture(t, cmd)
		assert.NotEmpty(t, stdout)
		assert.Empty(t, stderr)
	})

	// TODO(josh): Test with v23agentd on the path, too.
	t.Run("dump_existing_principal", func(t *testing.T) {
		homeDir, cleanUp := testutil.TempDir(t, "", "")
		defer cleanUp()
		principalDir := path.Join(homeDir, ".v23")
		principal, err := libsecurity.CreatePersistentPrincipal(principalDir, nil)
		assert.NoError(t, err)
		decoyPrincipalDir := path.Join(homeDir, "decoy_principal_dir")
		// Create a principal in the decoyPrincipalDir, as -dir still requires
		// a valid principal at $V23_CREDENTIALS.
		// TODO: Consider removing -dir flag, as this is surprising behavior.
		_, err = libsecurity.CreatePersistentPrincipal(decoyPrincipalDir, nil)
		assert.NoError(t, err)

		const blessingName = "grail-access-test-blessing-ln7z94"
		blessings, err := principal.BlessSelf(blessingName)
		assert.NoError(t, err)
		assert.NoError(t, principal.BlessingStore().SetDefault(blessings))

		t.Run("flag_dir", func(t *testing.T) {
			cmd := exec.Command(exe, "-dump", "-dir", principalDir)
			// Set $V23_CREDENTIALS to test that -dir takes priority.
			cmd.Env = []string{pathEnv, "V23_CREDENTIALS=" + decoyPrincipalDir}
			stdout, stderr := ticketServerUtil.RunAndCapture(t, cmd)
			assert.Contains(t, stdout, blessingName)
			assert.Empty(t, stderr)
		})

		t.Run("env_home", func(t *testing.T) {
			cmd := exec.Command(exe, "-dump")
			cmd.Env = []string{pathEnv, "HOME=" + homeDir}
			stdout, stderr := ticketServerUtil.RunAndCapture(t, cmd)
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
		stdout, stderr := ticketServerUtil.RunAndCapture(t, cmd)
		assert.Contains(t, stdout, blessingName)
		assert.Empty(t, stderr)
	})

	t.Run("fake_v23_servers", func(t *testing.T) {

		t.Run("ec2", func(t *testing.T) {
			const (
				wantDoc                 = "grailaccesstesttoken92lsl83"
				serverBlessingName      = "grail-access-test-blessing-laul37"
				clientBlessingExtension = "ec2-test"
				wantClientBlessing      = serverBlessingName + ":" + clientBlessingExtension
			)

			// Run fake ticket server: accepts EC2 instance identity document, returns blessings.
			var blesserEndpoint naming.Endpoint
			ctx, blesserEndpoint = ticketServerUtil.RunBlesserServer(ctx, t,
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
				"-blesser", fmt.Sprintf("/%s", blesserEndpoint.Address),
				"-ec2-instance-identity-url", fmt.Sprintf("http://%s/", listener.Addr().String()))
			cmd.Env = []string{pathEnv}
			stdout, _ := ticketServerUtil.RunAndCapture(t, cmd)
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
			ctx, blesserEndpoint = ticketServerUtil.RunBlesserServer(ctx, t,
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
				"-blesser", fmt.Sprintf("/%s", blesserEndpoint.Address),
				"-google-oauth2-url", fmt.Sprintf("http://%s", listener.Addr().String()))
			cmd.Env = []string{pathEnv}
			cmd.Stdin = bytes.NewReader([]byte("testcode"))
			stdout, _ := ticketServerUtil.RunAndCapture(t, cmd)
			assert.Contains(t, stdout, wantClientBlessing)

			// Make sure we got the right blessing.
			principal, err := libsecurity.LoadPersistentPrincipal(principalDir, nil)
			assert.NoError(t, err)
			defaultBlessing, _ := principal.BlessingStore().Default()
			assert.Contains(t, defaultBlessing.String(), wantClientBlessing)
		})

		t.Run("k8s", func(t *testing.T) {
			const (
				wantCaCrt               = "caCrt"
				wantNamespace           = "namespace"
				wantToken               = "token"
				wantRegion              = "us-west-2"
				serverBlessingName      = "grail-access-test-blessing-abc123"
				clientBlessingExtension = "k8s-test"
				wantClientBlessing      = serverBlessingName + ":" + clientBlessingExtension
			)

			// Run fake ticket server: accepts (caCrt, namespace, token), returns blessings.
			var blesserEndpoint naming.Endpoint
			ctx, blesserEndpoint = ticketServerUtil.RunBlesserServer(ctx, t,
				identity.K8sBlesserServer(fakeK8sBlesser(
					func(gotCaCrt string, gotNamespace string, gotToken string, gotRegion string, recipient security.PublicKey) security.Blessings {
						assert.Equal(t, gotCaCrt, wantCaCrt)
						assert.Equal(t, gotNamespace, wantNamespace)
						assert.Equal(t, gotToken, wantToken)
						assert.Equal(t, gotRegion, wantRegion)
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

			// Create caCrt, namespace, and token files
			tmpDir, cleanUp := testutil.TempDir(t, "", "")
			defer cleanUp()

			assert.NoError(t, ioutil.WriteFile(path.Join(tmpDir, "caCrt"), []byte(wantCaCrt), 0644))
			assert.NoError(t, ioutil.WriteFile(path.Join(tmpDir, "namespace"), []byte(wantNamespace), 0644))
			assert.NoError(t, ioutil.WriteFile(path.Join(tmpDir, "token"), []byte(wantToken), 0644))

			// Run grail-access to create a principal and bless it with the k8s flow.
			principalDir, principalCleanUp := testutil.TempDir(t, "", "")
			defer principalCleanUp()
			cmd := exec.Command(exe,
				"-dir", principalDir,
				"-blesser", fmt.Sprintf("/%s", blesserEndpoint.Address),
				"-k8s",
				"-ca-crt", path.Join(tmpDir, "caCrt"),
				"-namespace", path.Join(tmpDir, "namespace"),
				"-token", path.Join(tmpDir, "token"),
			)
			cmd.Env = []string{pathEnv}
			stdout, _ := ticketServerUtil.RunAndCapture(t, cmd)
			assert.Contains(t, stdout, wantClientBlessing)

			// Make sure we got the right blessing.
			principal, err := libsecurity.LoadPersistentPrincipal(principalDir, nil)
			assert.NoError(t, err)
			defaultBlessing, _ := principal.BlessingStore().Default()
			assert.Contains(t, defaultBlessing.String(), wantClientBlessing)
		})

		// If any of ca.crt, namespace, or token files are missing, an error should be thrown.
		t.Run("k8s_missing_file_should_fail", func(t *testing.T) {
			// Run grail-access to create a principal and bless it with the k8s flow.
			principalDir, principalCleanUp := testutil.TempDir(t, "", "")
			defer principalCleanUp()
			cmd := exec.Command(exe,
				"-dir", principalDir,
				"-k8s",
			)
			cmd.Env = []string{pathEnv}
			var stderrBuf strings.Builder
			cmd.Stderr = &stderrBuf
			err := cmd.Run()
			assert.Error(t, err)
			wantStderr := "no such file or directory"
			assert.True(t, strings.Contains(stderrBuf.String(), wantStderr))
		})

	})
}

type fakeBlesser func(arg string, recipientKey security.PublicKey) security.Blessings

func (f fakeBlesser) BlessEc2(_ *context.T, call rpc.ServerCall, s string) (security.Blessings, error) {
	return f(s, call.Security().RemoteBlessings().PublicKey()), nil
}

func (f fakeBlesser) BlessGoogle(_ *context.T, call rpc.ServerCall, s string) (security.Blessings, error) {
	return f(s, call.Security().RemoteBlessings().PublicKey()), nil
}

type fakeK8sBlesser func(arg1, arg2, arg3, arg4 string, recipientKey security.PublicKey) security.Blessings

func (f fakeK8sBlesser) BlessK8s(_ *context.T, call rpc.ServerCall, s1, s2, s3, s4 string) (security.Blessings, error) {
	return f(s1, s2, s3, s4, call.Security().RemoteBlessings().PublicKey()), nil
}
