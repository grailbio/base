// The following enables go generate to generate the doc.go file.
//go:generate go run v.io/x/lib/cmdline/gendoc "--build-cmd=go install" --copyright-notice= . -help
package main

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"time"

	_ "github.com/grailbio/base/cmdutil/interactive"
	"github.com/grailbio/base/security/ticket"
	_ "github.com/grailbio/v23/factories/grail" // Needed to initialize v23
	"v.io/v23"
	"v.io/v23/context"
	"v.io/v23/security"
	"v.io/v23/vom"
	"v.io/x/lib/cmdline"
	"v.io/x/lib/vlog"
	libsecurity "v.io/x/ref/lib/security"
	"v.io/x/ref/lib/v23cmd"
)

const blessingSuffix = "_role"

var (
	durationFlag time.Duration
	timeoutFlag  time.Duration
)

func newCmdRoot() *cmdline.Command {
	cmd := &cmdline.Command{
		Runner: v23cmd.RunnerFunc(run),
		Name:   "role",
		Short:  "Creates credentials for a role account",
		Long: `
Command role creates Vanadium principals for a Vanadium role account. This is
accomplished by fetching a VanadiumTicket from the ticket-server. The
ticket-server will bless the principal presented by the client so
the blessing presented to the ticket-server is required to have a ':_role'
prefix to prevent the accidental reuse of the original private key of the
client.

Example:

  grail role tickets/roles/lims-server /tmp/lims-server
`,
		ArgsName: "<ticket> <directory>",
	}
	cmd.Flags.DurationVar(&durationFlag, "duration", 1*time.Hour, "Duration for the blessing.")
	cmd.Flags.DurationVar(&timeoutFlag, "timeout", 10*time.Second, "The timeout of the requests to the server.")
	return cmd
}

func run(ctx *context.T, env *cmdline.Env, args []string) error {
	if len(args) != 2 {
		return fmt.Errorf("Exactly two arguments are required: <ticket> <directory>")
	}
	ticketPath, dir := args[0], args[1]

	principal, err := libsecurity.CreatePersistentPrincipal(dir, nil)
	if err != nil {
		return err
	}

	name, err := bless(ctx, principal, blessingSuffix)
	if err != nil {
		return err
	}

	if err := principal.BlessingStore().SetDefault(name); err != nil {
		return err
	}
	if _, err := principal.BlessingStore().Set(name, security.AllPrincipals); err != nil {
		return err
	}
	if err := security.AddToRoots(principal, name); err != nil {
		return err
	}

	vlog.Infof("\n%s", principal.BlessingStore().DebugString())

	roleCtx, err := v23.WithPrincipal(ctx, principal)
	if err != nil {
		return err
	}

	client := ticket.TicketServiceClient(ticketPath)
	_, cancel := context.WithTimeout(roleCtx, timeoutFlag)
	defer cancel()

	t, err := client.Get(roleCtx)
	if err != nil {
		return err
	}

	vlog.VI(1).Infof("%#v\n", t)

	vanadiumTicket, ok := t.(ticket.TicketVanadiumTicket)
	if !ok {
		return fmt.Errorf("Not a VanadiumTicket: %#v", t)
	}

	var blessings security.Blessings
	if err := base64urlVomDecode(vanadiumTicket.Value.Blessing, &blessings); err != nil {
		return err
	}

	vlog.Info(blessings)

	if err := principal.BlessingStore().SetDefault(blessings); err != nil {
		return err
	}
	if _, err := principal.BlessingStore().Set(blessings, "..."); err != nil {
		return err
	}
	if err := security.AddToRoots(principal, blessings); err != nil {
		return fmt.Errorf("failed to add blessings to recognized roots: %v", err)
	}

	fmt.Printf("Public key: %s\n", principal.PublicKey())
	fmt.Println("---------------- BlessingStore ----------------")
	fmt.Print(principal.BlessingStore().DebugString())
	fmt.Println("---------------- BlessingRoots ----------------")
	fmt.Print(principal.Roots().DebugString())

	return nil
}

func bless(ctx *context.T, p security.Principal, name string) (security.Blessings, error) {
	caveat, err := security.NewExpiryCaveat(time.Now().Add(durationFlag))
	if err != nil {
		ctx.Errorf("Couldn't create caveat")
		return security.Blessings{}, err
	}
	rp := v23.GetPrincipal(ctx)
	rblessing, _ := rp.BlessingStore().Default()
	return rp.Bless(p.PublicKey(), rblessing, name, caveat)
}

func base64urlVomDecode(s string, i interface{}) error {
	b, err := base64.URLEncoding.DecodeString(s)
	if err != nil {
		return err
	}
	dec := vom.NewDecoder(bytes.NewBuffer(b))
	return dec.Decode(i)
}

func main() {
	cmdline.HideGlobalFlagsExcept()
	cmdline.Main(newCmdRoot())
}
