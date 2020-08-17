package main

import (
	"fmt"
	"io/ioutil"
	"path/filepath"

	"github.com/grailbio/base/security/identity"
	"v.io/v23/context"
	"v.io/v23/security"
)

const defaultK8sBlesserFlag = "/ticket-server.eng.grail.com:8102/blesser/k8s"

func fetchK8sBlessings(ctx *context.T) (blessing security.Blessings, err error) {
	if blesserFlag == "" {
		blesserFlag = defaultK8sBlesserFlag
	}
	stub := identity.K8sBlesserClient(blesserFlag)

	caCrt, namespace, token, err := getFiles()
	if err != nil {
		return blessing, err
	}

	return stub.BlessK8s(ctx, caCrt, namespace, token, regionFlag)
}

func getFiles() (caCrt, namespace, token string, err error) {
	caCrtPath, err := filepath.Abs(caCrtFlag)
	if err != nil {
		return "", "", "", fmt.Errorf("parsing ca.crt path: %w", err)
	}
	namespacePath, err := filepath.Abs(namespaceFlag)
	if err != nil {
		return "", "", "", fmt.Errorf("parsing namespace path: %w", err)
	}
	tokenPath, err := filepath.Abs(tokenFlag)
	if err != nil {
		return "", "", "", fmt.Errorf("parsing token path: %w", err)
	}
	caCrtData, err := ioutil.ReadFile(caCrtPath)
	if err != nil {
		return "", "", "", fmt.Errorf("opening ca.crt: %w", err)
	}
	namespaceData, err := ioutil.ReadFile(namespacePath)
	if err != nil {
		return "", "", "", fmt.Errorf("opening ca.crt: %w", err)

	}
	tokenData, err := ioutil.ReadFile(tokenPath)
	if err != nil {
		return "", "", "", fmt.Errorf("opening ca.crt: %w", err)
	}

	return string(caCrtData), string(namespaceData), string(tokenData), err
}
