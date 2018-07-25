// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package v23data

import (
	"bytes"
	"encoding/base64"
	"fmt"

	"v.io/v23"
	"v.io/v23/context"
	"v.io/v23/security"
	"v.io/v23/vom"
	"v.io/x/lib/vlog"
)

const (
	// Under the hood these roots are public certs so having them in here poses no
	// security risks.
	//
	// Pipeline is using a different root.
	//
	//   Public key                                        Pattern
	//   59:20:19:97:dd:49:5e:3f:35:39:fe:d3:c7:d1:42:95   [pipeline]
	//
	// How the data was obtained:
	//
	//   principal -v23.credentials ~/.v23-pipeline-razvan get default | principal dumproots -
	pipelineRoot = "gV0cAgAUdi5pby92MjMvdW5pcXVlaWQuSWQBAgIQ4VsyBgAYdi5pby92MjMvc2VjdXJpdHkuQ2F2ZWF0AQIAAklkAS_hAAhQYXJhbVZvbQEn4eFZBAMBLuFhHAAAFnYuaW8vdjIzL3NlY3VyaXR5Lkhhc2gBA-FfQgYAG3YuaW8vdjIzL3NlY3VyaXR5LlNpZ25hdHVyZQEEAAdQdXJwb3NlASfhAARIYXNoATHhAAFSASfhAAFTASfh4VdZBgAddi5pby92MjMvc2VjdXJpdHkuQ2VydGlmaWNhdGUBBAAJRXh0ZW5zaW9uAQPhAAlQdWJsaWNLZXkBJ-EAB0NhdmVhdHMBLeEACVNpZ25hdHVyZQEw4eFVBAMBLOFTBAMBK-FROwYAH3YuaW8vdjIzL3NlY3VyaXR5LldpcmVCbGVzc2luZ3MBAQARQ2VydGlmaWNhdGVDaGFpbnMBKuHhUv--AAEBAAhwaXBlbGluZQFbMFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAEOP0MkPPMlEH4OiRm98-QotTsWWYgN229sjCJHq_TB9aolgRBFmpL0QI6qSa5E4SvAtMXuAuZqd9ackMBtWK1SAMAAkIxAQZTSEEyNTYCIPJJxI8quSubxV7xHQhoj_vfejvwUmDEu81pJzOSGbyoAyBboUYxo543p_sJOMf5NJoPngWHeDvGIAjBlWrt6RruQeHh4Q=="

	// Pipeline staging is using yet another different root.
	//
	//   Public key                                        Pattern
	//   b9:e1:c3:ef:26:b7:8a:88:86:8b:b7:2f:e8:d8:1b:7c   [staging-pipeline]
	pipelineStagingRoot = "gV0cAgAUdi5pby92MjMvdW5pcXVlaWQuSWQBAgIQ4VsyBgAYdi5pby92MjMvc2VjdXJpdHkuQ2F2ZWF0AQIAAklkAS_hAAhQYXJhbVZvbQEn4eFZBAMBLuFhHAAAFnYuaW8vdjIzL3NlY3VyaXR5Lkhhc2gBA-FfQgYAG3YuaW8vdjIzL3NlY3VyaXR5LlNpZ25hdHVyZQEEAAdQdXJwb3NlASfhAARIYXNoATHhAAFSASfhAAFTASfh4VdZBgAddi5pby92MjMvc2VjdXJpdHkuQ2VydGlmaWNhdGUBBAAJRXh0ZW5zaW9uAQPhAAlQdWJsaWNLZXkBJ-EAB0NhdmVhdHMBLeEACVNpZ25hdHVyZQEw4eFVBAMBLOFTBAMBK-FROwYAH3YuaW8vdjIzL3NlY3VyaXR5LldpcmVCbGVzc2luZ3MBAQARQ2VydGlmaWNhdGVDaGFpbnMBKuHhUv_GAAEBABBzdGFnaW5nLXBpcGVsaW5lAVswWTATBgcqhkjOPQIBBggqhkjOPQMBBwNCAARyGfli1xVYDZsy2puv2_cwERx_1JnRQxJ8HXmz2juBG3N61-U1gX1OazINr_MRTO5jBxs6ZNIQ7PxrZOjJ1RCHAwACQjEBBlNIQTI1NgIgpHN4wzd-h17Vps9k91N2rwrcQaQTs2pd2LvDhDnzzX8DIA6LRkRbhp7pGr2JBgGwqsbNgh9cfxdjmoETLpfBmR-h4eHh"
)

// InjectPipelineBlessings injects the non-v23.grail.com roots used by the
// pipeline and pipeline-staging. The ticket-server hand outs blessings to users
// and server using a v23.grail.com prefix. In order to allow the v23.grail.com
// blessings to talk to the pipeline and pipeline-staging we need to add these
// roots explicitly. Currently this is done by grail-access and grail-role, the
// two client tools that retrieve blessings from the ticket-server.
func InjectPipelineBlessings(ctx *context.T) error {
	principal := v23.GetPrincipal(ctx)

	pipeline, err := decodeBlessings(pipelineRoot)
	if err != nil {
		vlog.Error(err)
		return fmt.Errorf("failed to decode the pipeline root blessings: %v", err)
	}
	if err := security.AddToRoots(principal, pipeline); err != nil {
		vlog.Error(err)
		return fmt.Errorf("failed to add the pipeline root")
	}

	pipelineStaging, err := decodeBlessings(pipelineStagingRoot)
	if err != nil {
		vlog.Error(err)
		return fmt.Errorf("failed to decode the pipeline staging root blessings: %v", err)
	}
	if err := security.AddToRoots(principal, pipelineStaging); err != nil {
		vlog.Error(err)
		return fmt.Errorf("failed to add the pipeline staging root")
	}

	return nil
}

func decodeBlessings(s string) (security.Blessings, error) {
	b, err := base64.URLEncoding.DecodeString(s)
	if err != nil {
		return security.Blessings{}, err
	}

	dec := vom.NewDecoder(bytes.NewBuffer(b))
	var blessings security.Blessings
	return blessings, dec.Decode(&blessings)
}
