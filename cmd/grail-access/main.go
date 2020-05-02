// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// The following enables go generate to generate the doc.go file.
//go:generate go run v.io/x/lib/cmdline/gendoc "--build-cmd=go install" --copyright-notice= . -help
package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	_ "github.com/grailbio/v23/factories/grail"
	v23 "v.io/v23"
	v23context "v.io/v23/context"
	"v.io/v23/security"
	"v.io/x/lib/cmdline"
	"v.io/x/ref"
	"v.io/x/ref/lib/v23cmd"
	"v.io/x/ref/services/agent/agentlib"
)

const (
	// DATA(sensitive): These are the OAuth2 client ID and secret. They were
	// generated in the grail-razvanm Google Cloud Project. The client secret
	// is not secret in this case because it is part of client tool. It does act
	// as an identifier that allows restriction based on quota on the Google
	// side.
	clientID     = "27162366543-edih9cqc3t8p5hn9ord1k1n7h4oajfhm.apps.googleusercontent.com"
	clientSecret = "eRZyFfe5xJu0083zDk8Mlb6K"
)

var (
	credentialsDirFlag string
	ec2Flag            bool

	blesserGoogleFlag string
	browserFlag       bool
	googleOauth2Flag  string

	blesserEc2Flag          string
	ec2InstanceIdentityFlag string

	dumpFlag                 bool
	doNotRefreshDurationFlag time.Duration
)

func newCmdRoot() *cmdline.Command {
	cmd := &cmdline.Command{
		Runner: v23cmd.RunnerFunc(run),
		Name:   "grail-access",
		Short:  "Creates fresh Vanadium credentials",
		Long: `
Command grail-access creates Vanadium credentials (also called principals) using
either Google ID tokens (the default) or the AWS IAM role attached to an EC2
instance (requested using the '-ec2' flag).

For the Google-based auth the user will be prompted to go through an
OAuth flow that requires minimal permissions (only 'Know who you are
on Google') and obtains an ID token scoped to the clientID expected by
the server. The ID token is presented to the server via a Vanadium
RPC. For a 'xxx@grailbio.com' email address the server will hand to
the client a '[server]:google:xxx@grailbio.com' blessing where
'[server]' is the blessing of the server.

For the EC2-based auth an instance with ID 'i-0aec7b085f8432699' in the account
number '619867110810' using the 'adhoc' role the server will hand to the client
a '[server]:ec2:619867110810:role:adhoc:i-0aec7b085f8432699' blessing where
'server' is the blessing of the server.
`,
	}
	cmd.Flags.StringVar(&blesserGoogleFlag, "blesser-google", "/ticket-server.eng.grail.com:8102/blesser/google", "Blesser to talk to for the Google-based flow.")
	cmd.Flags.StringVar(&blesserEc2Flag, "blesser-ec2", "/ticket-server.eng.grail.com:8102/blesser/ec2", "Blesser to talk to for the EC2-based flow.")
	cmd.Flags.StringVar(&credentialsDirFlag, "dir", os.ExpandEnv("${HOME}/.v23"), "Where to store the Vanadium credentials. NOTE: the content will be erased if the credentials are regenerated.")
	cmd.Flags.BoolVar(&ec2Flag, "ec2", false, "Use the role of the EC2 VM.")
	cmd.Flags.StringVar(&ec2InstanceIdentityFlag, "ec2-instance-identity-url",
		"http://169.254.169.254/latest/dynamic/instance-identity/pkcs7",
		"URL for fetching instance identity document, for testing")
	cmd.Flags.BoolVar(&browserFlag, "browser", os.Getenv("SSH_CLIENT") == "", "Attempt to open a browser.")
	cmd.Flags.StringVar(&googleOauth2Flag, "google-oauth2-url",
		"https://accounts.google.com/o/oauth2",
		"URL for oauth2 API calls, for testing")
	cmd.Flags.BoolVar(&dumpFlag, "dump", false, "If credentials are present, dump them on the console instead of refreshing them.")
	cmd.Flags.DurationVar(&doNotRefreshDurationFlag, "do-not-refresh-duration", 7*24*time.Hour, "Do not refresh credentials if they are present and do not expire within this duration.")
	return cmd
}

func run(ctx *v23context.T, env *cmdline.Env, args []string) error {
	if _, ok := os.LookupEnv(ref.EnvCredentials); !ok {
		fmt.Printf("*******************************************************\n")
		fmt.Printf("*    WARNING: $V23_CREDENTIALS is not defined!        *\n")
		fmt.Printf("*******************************************************\n\n")
		fmt.Printf("How to fix this in bash: export V23_CREDENTIALS=%s\n\n", credentialsDirFlag)
	}
	agentPrincipal, err := agentlib.LoadPrincipal(credentialsDirFlag)
	if err == nil {
		// We have access to some credentials so we'll try to load them.
		ctx, err = v23.WithPrincipal(ctx, agentPrincipal)
		if err != nil {
			return err
		}
		agentBlessings, _ := agentPrincipal.BlessingStore().Default()
		if !agentBlessings.IsZero() {
			principal := v23.GetPrincipal(ctx)
			if err := principal.BlessingStore().SetDefault(agentBlessings); err != nil {
				return err
			}
			if err := security.AddToRoots(principal, agentBlessings); err != nil {
				return err
			}
		}
	} else {
		// We don't have access to credentials. Typically this happen on the first
		// run when the credentials directory is empty.

		// Dumping current credentials does not make sense when credentials are absent.
		if dumpFlag {
			fmt.Printf("Credentials not found in %s\n", credentialsDirFlag)
			return nil
		}
	}

	b, _ := v23.GetPrincipal(ctx).BlessingStore().Default()

	if dumpFlag || b.Expiry().After(time.Now().Add(doNotRefreshDurationFlag)) {
		dump(ctx)
		return nil
	}
	if ec2Flag {
		return runEc2(ctx)
	}
	return runGoogle(ctx)
}

func dump(ctx *v23context.T) {
	// Mimic the principal dump output.
	principal := v23.GetPrincipal(ctx)
	fmt.Printf("Public key: %s\n", principal.PublicKey())
	fmt.Println("---------------- BlessingStore ----------------")
	fmt.Print(principal.BlessingStore().DebugString())
	fmt.Println("---------------- BlessingRoots ----------------")
	fmt.Print(principal.Roots().DebugString())

	blessing, _ := principal.BlessingStore().Default()
	fmt.Printf("Expires on %s (in %s)\n", blessing.Expiry().Local(), time.Until(blessing.Expiry()))
}

func main() {
	// We disable this flag because it's initialized with the value of the
	// V23_CREDENTIALS environmental variable and that directory might be empty.
	if err := flag.Set("v23.credentials", ""); err != nil {
		panic(err)
	}
	// Prevent the v23agentd from running.
	if err := os.Setenv(ref.EnvCredentialsNoAgent, "1"); err != nil {
		panic(err)
	}
	cmdline.HideGlobalFlagsExcept()
	cmdline.Main(newCmdRoot())
}
