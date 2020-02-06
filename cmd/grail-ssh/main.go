// The following enables go generate to generate the doc.go file.
//go:generate go run v.io/x/lib/cmdline/gendoc "--build-cmd=go install" --copyright-notice= .
package main

import (
	"flag"
	"io"
	"os"

	"github.com/grailbio/base/cmdutil"
	_ "github.com/grailbio/base/cmdutil/interactive" // print output to console
	"github.com/grailbio/base/vcontext"
	_ "github.com/grailbio/v23/factories/grail"
	"v.io/v23/context"
	"v.io/x/lib/cmdline"
)

var (
	sshFlag   string
	idRsaFlag string
	userFlag  string
)

func newCmdRoot() *cmdline.Command {
	root := &cmdline.Command{
		Runner:   cmdutil.RunnerFuncWithAccessCheck(vcontext.Background, runner(runSsh)),
		Name:     "ssh",
		Short:    "ssh to a VM",
		ArgsName: "<ticket>",
		Long: `
Command that simplifies connecting to GRAIL systems using ssh with ssh certificates for authentication.
`,
		LookPath: false,
	}
	root.Flags.StringVar(&sshFlag, "ssh", "ssh", "What ssh client to use")
	root.Flags.StringVar(&idRsaFlag, "i", os.ExpandEnv("${HOME}/.ssh/id_rsa"), "Path to the SSH private key that will be used for the connection")
	root.Flags.StringVar(&userFlag, "l", "", "Username to provide to the remote host. If not provided selects the first principal defined as part of the ticket definition.")

	return root
}

type runnerFunc func(*context.T, io.Writer, *cmdline.Env, []string) error
type v23RunnerFunc func(*context.T, *cmdline.Env, []string) error

// runner wraps a runnerFunc to produce a cmdline.RunnerFunc.
func runner(f runnerFunc) v23RunnerFunc {
	return func(ctx *context.T, env *cmdline.Env, args []string) error {
		// No special actions needed that applies to all runners.
		return f(ctx, os.Stdout, env, args)
	}
}

func main() {
	// We suppress 'alsologtosterr' because this is a user tool.
	_ = flag.Set("alsologtostderr", "false")
	cmdRoot := newCmdRoot()
	cmdline.HideGlobalFlagsExcept()
	cmdline.Main(cmdRoot)
}
