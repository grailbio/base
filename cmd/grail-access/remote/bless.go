// Copyright 2022 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package remote

import (
	"bytes"
	"fmt"
	"os/exec"
	"strings"
	"text/template"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/ec2"
	"github.com/aws/aws-sdk-go/service/ec2/ec2iface"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/grailbio/base/cloud/awssession"
	"github.com/grailbio/base/must"
	v23 "v.io/v23"
	"v.io/v23/context"
	"v.io/v23/security"
)

const (
	// awsTicketPath is the path of the ticket that provides AWS credentials
	// for querying AWS/EC2 for running instances.
	awsTicketPath = "tickets/eng/dev/aws"
	// blessingsExtension is the extension added to the blessings sent to
	// remotes.
	blessingsExtension = "remote"

	// remoteExecS3Bucket is the bucket in which the known-compatible
	// grail-access binary installed on remote targets is stored.
	// TODO: Configure for an official build.
	remoteExecS3Bucket = "grail-bin-public"
	// remoteExecS3Key is the object key of the known-compatible grail-access
	// binary installed on remote targets.
	// TODO: Configure for an official build.
	// TODO: Stop assuming single platform (Linux/AMD64) of targets.
	remoteExecS3Key = "linux/amd64/2022-11-04.jcharumilind-181224/grail-access"
	// remoteExecExpiry is the expiry of the presigned URL we generate to
	// download (remoteExecS3Bucket, remoteExecS3Key).
	remoteExecExpiry = 15 * time.Minute
	// remoteExecSHA256 is the expected SHA-256 of the executable at
	// (remoteExecS3Bucket, remoteExecS3Key).
	// TODO: Set this to the official build's SHA-256.
	remoteExecSHA256 = "e64a0c3dabbea244297ae1d5cdb1811731914fac40e7acc9621979067f9291f5"
	// remoteExecPath is the path on the remote target at which we install and
	// later invoke the grail-access executable.  This string will be
	// double-quoted in a bash script, so variable expansions can be used.
	//
	// See XDG Base Directory Specification:
	// https://specifications.freedesktop.org/basedir-spec/basedir-spec-latest.html
	remoteExecPath = "${XDG_DATA_HOME:-${HOME}/.local/share}/grail-access/grail-access"
)

// Bless blesses the principals of targets with unconstrained extensions of
// the default blessings of the principal of ctx.  See package documentation
// (doc.go) for a description of target strings.
func Bless(ctx *context.T, targets []string) error {
	fmt.Println("---------------- Bless Remotes ----------------")
	sess, err := awssession.NewWithTicket(ctx, awsTicketPath)
	if err != nil {
		return fmt.Errorf("creating AWS session: %v", err)
	}
	dests, err := resolveTargets(ctx, sess, targets)
	if err != nil {
		return fmt.Errorf("resolving targets: %v", err)
	}
	p := v23.GetPrincipal(ctx)
	if p == nil {
		return fmt.Errorf("no local principal")
	}
	blessings, _ := p.BlessingStore().Default()
	for i, target := range targets {
		fmt.Printf("%s:\n", target)
		if len(dests[i]) == 0 {
			fmt.Println("  <no matching targets>")
			continue
		}
		for _, d := range dests[i] {
			if d.notRunning {
				fmt.Printf("  %-60s [ NOT RUNNING ]\n", d.s)
				continue
			}
			if err := blessSSHDest(ctx, sess, p, blessings, d.s); err != nil {
				return fmt.Errorf("blessing %q: %v", d.s, err)
			}
			fmt.Printf("  %-60s [ OK ]\n", d.s)
		}
	}
	return nil
}

type sshDest struct {
	s string
	// notRunning is whether we know that the host in s is not currently
	// running, e.g. because EC2 tells us so.  We use this to show nicer
	// status messages.
	notRunning bool
}

// blessSSHDest uses commands over SSH to bless dest's principal.  p is the
// blesser, and with are the blessings with which to bless dest's principal.
func blessSSHDest(
	ctx *context.T,
	sess *session.Session,
	p security.Principal,
	with security.Blessings,
	dest string,
) error {
	if err := ensureRemoteExec(ctx, sess, dest); err != nil {
		return fmt.Errorf("ensuring remote executable (grail-access) is available: %v", err)
	}
	key, err := remotePublicKey(ctx, dest)
	if err != nil {
		return fmt.Errorf("getting remote public key: %v", err)
	}
	blessingSelf, err := keysEqual(key, p.PublicKey())
	if err != nil {
		return fmt.Errorf("checking if blessing self: %v", err)
	}
	if blessingSelf {
		return fmt.Errorf("cannot bless self; check that target is a remote machine/principal")
	}
	b, err := p.Bless(key, with, blessingsExtension, security.UnconstrainedUse())
	if err != nil {
		return fmt.Errorf("blessing %v with %v: %v", key, with, err)
	}
	if err := sendBlessings(ctx, b, dest); err != nil {
		return fmt.Errorf("sending blessings to %s: %v", dest, err)
	}
	return nil
}

func ensureRemoteExec(ctx *context.T, sess *session.Session, dest string) error {
	script, err := makeEnsureRemoteExecScript(sess)
	if err != nil {
		return fmt.Errorf(
			"making script to ensure remote grail-access executable is available: %v",
			err,
		)
	}
	cmd := sshCommand(ctx, dest, "bash -s")
	cmd.Stdin = strings.NewReader(script)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf(
			"running installation script on %q: %v"+
				"\n--- std{err,out} ---\n%s",
			dest,
			err,
			output,
		)
	}
	return nil
}

func makeEnsureRemoteExecScript(sess *session.Session) (string, error) {
	url, err := presignRemoteExecURL(sess)
	if err != nil {
		return "", fmt.Errorf("presigning URL of grail-access executable: %v", err)
	}
	// "Escape" single quotes, as we enclose the URL in single quotes in our
	// generated script.
	url = strings.ReplaceAll(url, "'", "'\\''")
	var b strings.Builder
	ensureRemoteExecTemplate.Execute(&b, map[string]string{
		"url":    url,
		"sha256": remoteExecSHA256,
		"path":   remoteExecPath,
	})
	return b.String(), nil
}

// ensureRemoteExecTemplate is the template for building the script used to
// ensure that the remote has a compatible grail-access binary installed.  We
// inject the configuration for installation.
var ensureRemoteExecTemplate *template.Template

func init() {
	must.True(!strings.Contains(remoteExecSHA256, "'"))
	ensureRemoteExecTemplate = template.Must(template.New("script").Parse(`
set -euxo pipefail

# url is the S3 URL from which to fetch the grail-access binary that will run
# on the target.
url='{{.url}}'
# sha256 is the expected SHA-256 hash of the grail-access binary.
sha256='{{.sha256}}'

# path is the path at which will we ultimately place the grail-access binary.
path="{{.path}}"
dir="$(dirname "${path}")"

sha_bad=0
echo "${sha256} ${path}" | sha256sum --check --quiet - || sha_bad=$?
if [ $sha_bad == 0 ]; then
	# We already have the right binary.  Ensure that it is executable.  This
	# should be a no-op unless it was changed externally.
	chmod 700 "${path}"
	exit
fi

mkdir --mode=700 --parents "${dir}"
chmod 700 "${dir}"
path_download="$(mktemp "${path}.XXXXXXXXXX")"
trap "rm --force -- \"${path_download}\"" EXIT
curl --fail "${url}" --output "${path_download}"
echo "${sha256} ${path_download}" | sha256sum --check --quiet -
chmod 700 "${path_download}"
mv --force "${path_download}" "${path}"
`))
}

func remotePublicKey(ctx *context.T, dest string) (security.PublicKey, error) {
	var (
		cmd    = remoteExecCommand(ctx, dest, ModePublicKey)
		stderr bytes.Buffer
	)
	cmd.Stderr = &stderr
	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf(
			"running grail-access(in mode: %s) on remote: %v;"+
				"\n--- stderr ---\n%s",
			ModePublicKey,
			err,
			stderr.String(),
		)
	}
	key, err := decodePublicKey(string(output))
	if err != nil {
		return nil, fmt.Errorf("decoding public key %q: %v", string(output), err)
	}
	return key, nil
}

func keysEqual(lhs, rhs security.PublicKey) (bool, error) {
	lhsBytes, err := lhs.MarshalBinary()
	if err != nil {
		return false, fmt.Errorf("left-hand side of comparison invalid: %v", err)
	}
	rhsBytes, err := rhs.MarshalBinary()
	if err != nil {
		return false, fmt.Errorf("right-hand side of comparison invalid: %v", err)
	}
	return bytes.Equal(lhsBytes, rhsBytes), nil
}

func sendBlessings(ctx *context.T, b security.Blessings, dest string) error {
	var (
		cmd                  = remoteExecCommand(ctx, dest, ModeReceive)
		blessingsString, err = encodeBlessings(b)
	)
	if err != nil {
		return fmt.Errorf("encoding blessings: %v", err)
	}
	_ = blessingsString
	cmd.Stdin = strings.NewReader(blessingsString)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf(
			"running grail-access(in mode: %s) on remote: %v;"+
				"\n--- stderr ---\n%s",
			ModeReceive,
			err,
			stderr.String(),
		)
	}
	return nil
}

func remoteExecCommand(ctx *context.T, dest, mode string) *exec.Cmd {
	return sshCommand(
		ctx,
		dest,
		// Set a reasonable value V23_CREDENTIALS in case the target's bash
		// does not configure it (in non-login shells).
		"V23_CREDENTIALS=${HOME}/.v23",
		remoteExecPath, "-"+FlagNameMode+"="+mode,
	)
}

func sshCommand(ctx *context.T, dest string, args ...string) *exec.Cmd {
	cmdArgs := []string{
		// Use batch mode which prevents prompting for an SSH passphrase.  The
		// prompt is more confusing than failing outright, as we run multiple
		// SSH commands, so even if the user enters the correct passphrase,
		// they will see more prompts.
		"-o", "BatchMode yes",
		// Don't check the identity of the remote host.
		"-o", "StrictHostKeyChecking no",
		// Don't store the identity of the remote host.
		"-o", "UserKnownHostsFile /dev/null",
		dest,
	}
	cmdArgs = append(cmdArgs, args...)
	return exec.CommandContext(ctx, "ssh", cmdArgs...)
}

// resolveTargets resolves targets into SSH destinations.  Destinations are
// returned as a two-dimensional slice of length len(targets).  Each entry
// corresponds to the input target and is a slice of the matching SSH
// destinations, if any.
//
// Note that for ec2-name targets, we make API calls to EC2 to resolve the
// corresponding hosts.  A single ec2-name target may resolve to multiple (or
// zero) SSH destinations, as names are given as filters.
func resolveTargets(ctx *context.T, sess *session.Session, targets []string) ([][]sshDest, error) {
	var dests = make([][]sshDest, len(targets))
	for i, target := range targets {
		parts := strings.SplitN(target, ":", 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("target not in \"type:value\" format: %v", target)
		}
		var (
			typ    = parts[0]
			val    = parts[1]
			ec2API = ec2.New(sess)
		)
		switch typ {
		case "ssh":
			dests[i] = append(dests[i], sshDest{s: val})
		case "ec2-name":
			ec2Dests, err := resolveEC2Target(ctx, ec2API, val)
			if err != nil {
				return nil, fmt.Errorf("resolving EC2 target %v: %v", val, err)
			}
			dests[i] = append(dests[i], ec2Dests...)
		default:
			return nil, fmt.Errorf("invalid target type for %q: %v", target, typ)
		}
	}
	return dests, nil
}

func resolveEC2Target(ctx *context.T, ec2API ec2iface.EC2API, s string) ([]sshDest, error) {
	var (
		user string
		name string
	)
	parts := strings.SplitN(s, "@", 2)
	switch len(parts) {
	case 1:
		user = "ubuntu"
		name = parts[0]
	case 2:
		user = parts[0]
		name = parts[1]
	default:
		must.Never("SplitN returned invalid result")
	}
	instances, err := findInstances(ctx, ec2API, name)
	if err != nil {
		return nil, fmt.Errorf("finding instances matching %q: %v", name, err)
	}
	var dests []sshDest
	for _, i := range instances {
		var (
			s          = fmt.Sprintf("%s@%s", user, *i.PrivateIpAddress)
			notRunning = *i.State.Name != ec2.InstanceStateNameRunning
		)
		dests = append(dests, sshDest{s: s, notRunning: notRunning})
	}
	return dests, nil
}

func presignRemoteExecURL(sess *session.Session) (string, error) {
	s3API := s3.New(sess)
	req, _ := s3API.GetObjectRequest(&s3.GetObjectInput{
		Bucket: aws.String(remoteExecS3Bucket),
		Key:    aws.String(remoteExecS3Key),
	})
	url, err := req.Presign(remoteExecExpiry)
	if err != nil {
		return "", fmt.Errorf(
			"presigning URL for s3://%s/%s: %v",
			remoteExecS3Bucket,
			remoteExecS3Key,
			err,
		)
	}
	return url, nil
}

func findInstances(ctx *context.T, api ec2iface.EC2API, name string) ([]*ec2.Instance, error) {
	input := &ec2.DescribeInstancesInput{
		Filters: []*ec2.Filter{
			{
				Name:   aws.String("tag:Name"),
				Values: aws.StringSlice([]string{name}),
			},
		},
	}
	output, err := api.DescribeInstancesWithContext(ctx, input)
	if err != nil {
		return nil, fmt.Errorf(
			"DescribeInstances error:\n%v\nDescribeInstances request:\n%v",
			err,
			input,
		)
	}
	return reservationsInstances(output.Reservations), nil
}

func reservationsInstances(reservations []*ec2.Reservation) []*ec2.Instance {
	instances := []*ec2.Instance{}
	for _, r := range reservations {
		instances = append(instances, r.Instances...)
	}
	return instances
}
