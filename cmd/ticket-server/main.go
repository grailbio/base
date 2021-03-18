// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// The following enables go generate to generate the doc.go file.
//go:generate go run v.io/x/lib/cmdline/gendoc "--build-cmd=go install" --copyright-notice= . -help
package main

import (
	"errors"
	"io/ioutil"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/grailbio/base/cmd/ticket-server/config"
	"github.com/grailbio/base/common/log"
	"github.com/grailbio/base/security/identity"
	_ "github.com/grailbio/base/security/keycrypt/file"
	_ "github.com/grailbio/base/security/keycrypt/keychain"
	_ "github.com/grailbio/base/security/keycrypt/kms"
	"github.com/grailbio/base/security/ticket"
	_ "github.com/grailbio/v23/factories/grail"
	"golang.org/x/oauth2/google"
	"golang.org/x/oauth2/jwt"
	admin "google.golang.org/api/admin/directory/v1"
	v23 "v.io/v23"
	"v.io/v23/context"
	"v.io/v23/glob"
	"v.io/v23/naming"
	"v.io/v23/rpc"
	"v.io/v23/security"
	"v.io/x/lib/cmdline"
	"v.io/x/ref/lib/security/securityflag"
	"v.io/x/ref/lib/signals"
	"v.io/x/ref/lib/v23cmd"
)

var (
	nameFlag            string
	configDirFlag       string
	regionFlag          string
	googleUserSufixFlag string
	googleAdminNameFlag string

	dryrunFlag bool

	googleExpirationIntervalFlag time.Duration
	serviceAccountFlag           string

	ec2BlesserRoleFlag             string
	ec2ExpirationIntervalFlag      time.Duration
	ec2DynamoDBTableFlag           string
	ec2DisableAddrCheckFlag        bool
	ec2DisableUniquenessCheckFlag  bool
	ec2DisablePendingTimeCheckFlag bool

	k8sBlesserRoleFlag        string
	k8sExpirationIntervalFlag time.Duration
	awsAccountsFlag           string
	awsRegionsFlag            string
)

func newCmdRoot() *cmdline.Command {
	root := &cmdline.Command{
		Runner: v23cmd.RunnerFunc(run),
		Name:   "ticket-server",
		Short:  "Runs a Vanadium server that allows restricted access to tickets",
		Long: `
Command ticket-server runs a Vanadium server that provides restricted access to
tickets. A ticket contains credentials and configurations that allows
communicating with another system. For example, an S3 ticket contains AWS
credentials and also the bucket and object or prefix to fetch while a Docker
ticket contains the TLS certificate expected from the server, a client TLS
certificate + the private key and the URL to reach the Docker daemon.
`,
	}
	root.Flags.StringVar(&nameFlag, "name", "", "Name to mount the server under. If empty, don't mount.")
	root.Flags.StringVar(&configDirFlag, "config-dir", "", "Directory with tickets in VDL format. Must be provided.")
	root.Flags.BoolVar(&dryrunFlag, "dry-run", false, "Don't run, just check the configs.")
	root.Flags.StringVar(&regionFlag, "region", "us-west-2", "AWS region to use for cached AWS session.")
	root.Flags.DurationVar(&googleExpirationIntervalFlag, "google-expiration", 7*24*time.Hour, "Expiration caveat for the Google-based blessings.")
	root.Flags.StringVar(&serviceAccountFlag, "service-account", "", "JSON file with a Google service account credentials.")
	root.Flags.StringVar(&ec2BlesserRoleFlag, "ec2-blesser-role", "", "What role to use for the blesser/ec2 endpoint. The role needs to exist in all the accounts.")
	root.Flags.DurationVar(&ec2ExpirationIntervalFlag, "ec2-expiration", 365*24*time.Hour, "Expiration caveat for the EC2-based blessings.")
	root.Flags.StringVar(&ec2DynamoDBTableFlag, "ec2-dynamodb-table", "", "DynamoDB table to use for enforcing the uniqueness of the EC2-based blessings requests.")
	root.Flags.BoolVar(&ec2DisableAddrCheckFlag, "danger-danger-danger-ec2-disable-address-check", false, "Disable the IP address check for the EC2-based blessings requests. Only useful for local tests.")
	root.Flags.BoolVar(&ec2DisableUniquenessCheckFlag, "danger-danger-danger-ec2-disable-uniqueness-check", false, "Disable the uniqueness check for the EC2-based blessings requests. Only useful for local tests.")
	root.Flags.BoolVar(&ec2DisablePendingTimeCheckFlag, "danger-danger-danger-ec2-disable-pending-time-check", false, "Disable the pendint time check for the EC2-based blessings requests. Only useful for local tests.")

	root.Flags.StringVar(&googleUserSufixFlag, "google-user-domain", "grailbio.com", "Comma-separated list of email domains used for validating users.")
	root.Flags.StringVar(&googleAdminNameFlag, "google-admin", "admin@grailbio.com", "Google Admin that can read all group memberships - NOTE: all groups will need to match the admin user's domain.")

	root.Flags.DurationVar(&k8sExpirationIntervalFlag, "k8s-expiration", 365*24*time.Hour, "Expiration caveat for the K8s-based blessings.")
	root.Flags.StringVar(&k8sBlesserRoleFlag, "k8s-blesser-role", "ticket-server", "What role to use to lookup EKS cluster information on all authorized accounts. The role needs to exist in all the accounts.")
	root.Flags.StringVar(&awsAccountsFlag, "aws-account-ids", "", "Commma-separated list of AWS account IDs used to populate allow-list of k8s clusters.")
	root.Flags.StringVar(&awsRegionsFlag, "aws-regions", "us-west-2", "Commma-separated list of AWS regions used to populate allow-list of k8s clusters.")

	return root
}

// node describes an inner node in the config tree. The leaves are of type
// service.
type node struct {
	name     string
	children map[string]interface{}
}

var _ rpc.AllGlobber = (*node)(nil)

func (n *node) Glob__(ctx *context.T, call rpc.GlobServerCall, g *glob.Glob) error { // nolint: golint
	log.Info(ctx, "glob request", "glob", g, "blessing", call.Security().RemoteBlessings(), "ticket", call.Suffix())

	sender := call.SendStream()
	element := g.Head()

	// The key is the path to a node.
	children := map[string]interface{}{"": n}
	for g.Len() != 0 {
		children = descent(children)
		matches := map[string]interface{}{}
		for k, v := range children {
			v := v.(*node)
			if element.Match(v.name) {
				matches[k] = v
			}
		}
		children = matches
		g = g.Tail()
		element = g.Head()
	}

	if g.String() == "..." {
		matches := map[string]interface{}{}
		for k1, v1 := range children {
			v1 := v1.(*node)
			for k2, v2 := range v1.flatten(k1) {
				matches[k2] = v2
			}
		}
		children = matches
	}

	for k, v := range children {
		isLeaf := false
		switch v.(type) {
		case *node:
			isLeaf = len(v.(*node).children) == 0
		case *entry:
			isLeaf = true
		}
		sender.Send(naming.GlobReplyEntry{
			Value: naming.MountEntry{
				Name:   strings.TrimLeft(k, "/"),
				IsLeaf: isLeaf,
			},
		})
	}

	return nil
}

// flatten expands a node recursively. A node "a" with two empty children "b"
// and "c" should return a map with keys: "a", "a/b", "a/c". The values are
// pointers to node structs.
func (n *node) flatten(prefix string) map[string]interface{} {
	r := map[string]interface{}{}
	for _, v1 := range n.children {
		v1 := v1.(*node)
		k1 := naming.Join(prefix, v1.name)
		r[k1] = v1
		for k2, v2 := range v1.flatten(k1) {
			v2 := v2.(*node)
			r[k2] = v2
		}
	}
	return r
}

func descent(m map[string]interface{}) map[string]interface{} {
	r := map[string]interface{}{}
	for k1, v1 := range m {
		v1 := v1.(*node)
		for k2, v2 := range v1.children {
			r[k1+"/"+k2] = v2
		}
	}
	return r
}

type entry struct {
	kind    string
	service interface{}
	auth    security.Authorizer
}

type dispatcher struct {
	registry map[string]entry
	root     *node
}

var d *dispatcher

func newDispatcher(ctx *context.T, awsSession *session.Session, cfg config.Config, jwtConfig *jwt.Config) rpc.Dispatcher {
	d = &dispatcher{
		registry: make(map[string]entry),
		root:     &node{},
	}

	// Note that the blesser/ endpoints are not exposed via Glob__ and the
	// permissions are governed by the -v23.permissions.{file,literal} flags.
	d.registry["blesser/google"] = entry{
		service: identity.GoogleBlesserServer(newGoogleBlesser(ctx, googleExpirationIntervalFlag,
			strings.Split(googleUserSufixFlag, ","))),
		auth: securityflag.NewAuthorizerOrDie(ctx),
	}
	d.registry["blesser/k8s"] = entry{
		service: identity.K8sBlesserServer(newK8sBlesser(awsSession, k8sExpirationIntervalFlag, k8sBlesserRoleFlag, strings.Split(awsAccountsFlag, ","), strings.Split(awsRegionsFlag, ","))),
		auth:    securityflag.NewAuthorizerOrDie(ctx),
	}
	if ec2BlesserRoleFlag != "" {
		d.registry["blesser/ec2"] = entry{
			service: identity.Ec2BlesserServer(newEc2Blesser(ctx, awsSession, ec2ExpirationIntervalFlag, ec2BlesserRoleFlag, ec2DynamoDBTableFlag)),
			auth:    securityflag.NewAuthorizerOrDie(ctx),
		}
	}
	d.registry["list"] = entry{
		service: ticket.ListServiceServer(newList(ctx)),
		auth:    securityflag.NewAuthorizerOrDie(ctx),
	}

	for k, v := range cfg {
		auth := googleGroupsAuthorizer(ctx, v.Perms, jwtConfig, googleAdminNameFlag)
		log.Debug(ctx, "adding service to dispatcher registry", "name", k, "perms", v.Perms)
		parts := strings.Split(k, "/")
		n := d.root
		for _, p := range parts {
			if n.children == nil {
				n.children = map[string]interface{}{}
			}
			if next, ok := n.children[p]; ok {
				n = next.(*node)
			} else {
				n.children[p] = &node{name: p}
				n = n.children[p].(*node)
			}
		}
		d.registry[k] = entry{
			service: ticket.TicketServiceServer(&service{
				name:       parts[len(parts)-1],
				kind:       v.Kind,
				ticket:     v.Ticket,
				perms:      v.Perms,
				awsSession: awsSession,
			}),
			auth: auth,
		}
	}
	return d
}

// Lookup implements the Dispatcher interface from v.io/v23/rpc.
func (d *dispatcher) Lookup(ctx *context.T, suffix string) (interface{}, security.Authorizer, error) {
	log.Debug(ctx, "performing service looking", "name", suffix)
	if s, ok := d.registry[suffix]; ok {
		return s.service, s.auth, nil
	}
	return d.root, security.DefaultAuthorizer(), nil
}

func run(ctx *context.T, env *cmdline.Env, args []string) error {
	if configDirFlag == "" {
		return errors.New("-config-dir flag is required")
	}

	ticketConfig, err := config.Load(configDirFlag)
	if err != nil {
		return err
	}

	if dryrunFlag {
		return nil
	}

	if serviceAccountFlag == "" {
		return errors.New("-service-account flag is required")
	}

	blessings, _ := v23.GetPrincipal(ctx).BlessingStore().Default()
	log.Debug(ctx, "using default blessing", "blessing", blessings)

	awsSession, err := session.NewSession(aws.NewConfig().WithRegion(regionFlag))
	if err != nil {
		return err
	}

	serviceAccountJSON, err := ioutil.ReadFile(serviceAccountFlag)
	if err != nil {
		return err
	}
	jwtConfig, err := google.JWTConfigFromJSON(serviceAccountJSON, admin.AdminDirectoryGroupMemberReadonlyScope+" "+admin.AdminDirectoryGroupReadonlyScope)
	if err != nil {
		return err
	}

	dispatcher := newDispatcher(ctx, awsSession, ticketConfig, jwtConfig)
	_, s, err := v23.WithNewDispatchingServer(ctx, nameFlag, dispatcher)
	if err != nil {
		return err
	}

	for _, endpoint := range s.Status().Endpoints {
		log.Info(ctx, "server endpoint", "addr", endpoint.Name())
	}
	<-signals.ShutdownOnSignals(ctx) // Wait forever.
	return nil
}

func main() {
	cmdline.HideGlobalFlagsExcept()
	cmdline.Main(newCmdRoot())
}
