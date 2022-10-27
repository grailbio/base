// Copyright 2022 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package remote

const (
	// FlagNameMode is the name of the string flag used to set the mode of
	// grail-access for sending and receiving blessings.
	FlagNameMode = "internal-bless-remotes-mode"
	// ModeSend initiates the full sender workflow.  See package documentation.
	ModeSend = "send"
	// ModePublicKey causes grail-access to print the local principal's public
	// key.
	ModePublicKey = "public-key"
	// ModeReceive causes grail-access to read blessings from os.Stdin and set
	// them as both the default and for all principal peers.
	ModeReceive = "receive"
)
