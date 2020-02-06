package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"syscall"
	"time"

	"github.com/grailbio/base/security/ticket"
	sshLib "golang.org/x/crypto/ssh"
	terminal "golang.org/x/crypto/ssh/terminal"
	"v.io/v23/context"
	"v.io/x/lib/cmdline"
	"v.io/x/lib/vlog"
)

const (
	timeout = 10 * time.Second
)

func runSsh(ctx *context.T, out io.Writer, env *cmdline.Env, args []string) error {
	if len(args) == 0 {
		return env.UsageErrorf("At least one argument (<ticket>) is required.")
	}

	ticketPath := args[0]
	args = args[1:] // remove the ticket from the arguments

	client := ticket.TicketServiceClient(ticketPath)
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// Read in the private key
	privateKey, err := ioutil.ReadFile(idRsaFlag)
	if err != nil {
		return fmt.Errorf("Failed to read private key - %v", err)
	}

	// Load the private key
	privateSigner, err := sshLib.ParsePrivateKey(privateKey)
	if err != nil {
		switch err.(type) {
		case *sshLib.PassphraseMissingError:
			// try to load the key with a passphrase
			fmt.Print("Enter SSH Key Passphrase: ")
			bytePassword, _ := terminal.ReadPassword(int(syscall.Stdin))
			privateSigner, err = sshLib.ParsePrivateKeyWithPassphrase(privateKey, bytePassword)
			if err != nil {
				return fmt.Errorf("Failed to read private key - %v", err)
			}
			fmt.Println("\nSSH Key decoded")
		default:
			return fmt.Errorf("Failed to parse private key - %v", err)
		}
	}

	if err != nil {
		return fmt.Errorf("Failed to parse private key - %v", err)
	}

	var parameters = []ticket.Parameter{
		ticket.Parameter{
			Key:   "PublicKey",
			Value: string(sshLib.MarshalAuthorizedKey(privateSigner.PublicKey())),
		},
	}

	t, err := client.GetWithParameters(ctx, parameters)
	if err != nil {
		return fmt.Errorf("Failed to communicate with the ticket-server - %v", err)
	}

	switch t.Index() {
	case (ticket.TicketSshCertificateTicket{}).Index():
		{
			creds := t.(ticket.TicketSshCertificateTicket).Value.Credentials
			// pull the public certificate out and write to the id_rsa cert path location
			if err = ioutil.WriteFile(idRsaFlag+"-cert.pub", []byte(creds.Cert), 0644); err != nil {
				return fmt.Errorf("Failed to write ssh public key "+idRsaFlag+"-cert.pub"+" - %v", err)
			}
		}
	default:
		{
			return fmt.Errorf("Provided ticket is not a SSHCertificateTicket")
		}
	}

	var computeInstances []ticket.ComputeInstance = t.(ticket.TicketSshCertificateTicket).Value.ComputeInstances
	var username = t.(ticket.TicketSshCertificateTicket).Value.Username
	// Use the environment provided username if specified
	if userFlag != "" {
		username = userFlag
	}
	// Throw an error if no username is set
	if username == "" {
		vlog.Errorf("Username was not provided in ticket or via command line")
		// TODO: return the exit code from the cmd.
		os.Exit(1)
	}

	var host string
	instanceMatch := regexp.MustCompile("^i-[a-zA-Z0-9]+$")
	// Not the best regex (e.g. doesn't match IPV6) to use here ... better regexs are available at
	// https://stackoverflow.com/questions/106179/regular-expression-to-match-dns-hostname-or-ip-address
	hostIpMatch := regexp.MustCompile("^([a-zA-Z0-9]+\\.)+[a-zA-Z0-9]+$")
	stopMatch := regexp.MustCompile("^--$")

	// Loop through the arguments provided to the CLI tool - and try to match to a hostname or an instanceID.
	// Stop processing if -- is found.
	// host is the last match found.
	for i, arg := range args {
		match := instanceMatch.MatchString(arg)
		if err != nil {
			return fmt.Errorf("Failed to check if input %s matched an instanceId - %v", arg, err)
		}
		// Find matching instanceId in list
		if match {
			// Remove the matched element from the list
			args = append(args[:i], args[i+1:]...)
			for _, instance := range computeInstances {
				if instance.InstanceId == arg {
					vlog.Errorf("Matched InstanceID %s - %s", instance.InstanceId, instance.PublicIp)
					host = instance.PublicIp
					break
				}
			}
			if host == "" {
				return fmt.Errorf("Failed to find a match for InstanceId provided %s", arg)
			}
			break
		}

		// check for a dns/ip host name to stop processing
		match = hostIpMatch.MatchString(arg)
		if err != nil {
			return fmt.Errorf("Failed to check if input %s matched an '^[a-zA-Z0-9]+\\.[a-zA-Z0-9]+' - %v", arg, err)
		}
		if match {
			host = arg
			args = append(args[:i], args[i+1:]...)
			break
		}

		// check for a -- to stop processing
		match = stopMatch.MatchString(arg)
		if err != nil {
			return fmt.Errorf("Failed to check if input %s matched an '--' - %v", arg, err)
		}
		if match {
			break
		}
	}

	// If no host has been found present a list
	if host == "" {
		fmt.Printf("No host or InstanceId provided - please select from list provided by the ticket")
		// prompt for which instance to connect too
		for index, instance := range computeInstances {
			fmt.Printf("[%d] %s:%s - %s\n", index, instance.InstanceId, getTagValueFromKey(instance, "Name"), instance.PublicIp)
		}
		var instanceSelection int = -1 // initialize to negative value
		fmt.Printf("Enter number for corresponding system to connect to?")
		if _, err := fmt.Scanf("%d", &instanceSelection); err != nil {
			return err
		}

		if instanceSelection < 0 || instanceSelection > len(computeInstances) {
			return fmt.Errorf("Selected index (%d) was not in the list", instanceSelection)
		}
		if computeInstances[instanceSelection].PublicIp != "" {
			host = computeInstances[instanceSelection].PublicIp
		} else {
			host = computeInstances[instanceSelection].PrivateIp
		}
	}

	if host == "" {
		return fmt.Errorf("Host selection failed - please provide an ip, DNS name, or select host from list with no input")
	}

	var sshArgs = []string{
		// Forward the ssh agent.
		"-A",
		// Forward the X11 connections.
		"-X",
		// Don't check the identity of the remote host.
		"-o", "StrictHostKeyChecking no",
		// Don't store the identity of the remote host.
		"-o", "UserKnownHostsFile /dev/null",
		// Pass the private key to the ssh command
		"-i", idRsaFlag,
	}

	// When using MOSH, SSH connection commands need to be passed like
	// $ mosh --ssh="ssh -i ./identity" username@host
	if sshFlag == "mosh" {
		var moshSshArg = strings.Join(sshArgs, " ")
		sshArgs = []string{
			"--ssh", moshSshArg,
		}
	}

	sshArgs = append(sshArgs,
		username+"@"+host,
	)

	sshArgs = append(sshArgs, args...)

	vlog.Infof("exec: %q %q", sshFlag, sshArgs)
	cmd := exec.Command(sshFlag, sshArgs...)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		vlog.Errorf("ssh error: %s", err)
		// TODO: return the exit code from the cmd.
		os.Exit(1)
	}

	return nil
}

// Return the key value from the list of Tag Parameters
func getTagValueFromKey(instance ticket.ComputeInstance, key string) string {
	for _, param := range instance.Tags {
		if param.Key == key {
			return param.Value
		}
	}

	// Throwing a NoSuchKey value is overkill for cases where tag is not added
	return ""
}
