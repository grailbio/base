// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package cmdutil

import (
	"bufio"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// TicketFlags represents an implementation of flag.Value that can be used
// to specify tickets either in comma separated form or by repeating the
// same flag. That is, either:
//   --flag=t1,t2,t3
// and/or
//   --flag=t1 --flag=t2 --flag=t3
// and/or
//   --flag=t1,t2 --flag=t3
type TicketFlags struct {
	set                bool
	dedup              map[string]bool
	fs                 *flag.FlagSet
	ticketFlag, rcFlag string
	Tickets            []string
	TicketRCFile       string
	ticketRCFlag       stringFlag
	Timeout            time.Duration
}

// wrapper to catch explicit setting of a flag.
type stringFlag struct {
	set  bool
	name string
	val  *string
}

// Set implements flag.Value.
func (sf *stringFlag) Set(v string) error {
	sf.set = true
	*sf.val = v
	return nil
}

// String implements flag.Value.
func (sf *stringFlag) String() string {
	if sf.val == nil {
		// called via flag.isZeroValue.
		return ""
	}
	return *sf.val
}

// Set implements flag.Value.
func (tf *TicketFlags) Set(v string) error {
	if !tf.set {
		// Clear any defaults if setting for the first time.
		tf.Tickets = nil
		tf.dedup = map[string]bool{}
	}
	for _, ps := range strings.Split(v, ",") {
		if ps == "" {
			continue
		}
		if !tf.dedup[ps] {
			tf.Tickets = append(tf.Tickets, ps)
		}
		tf.dedup[ps] = true
	}
	tf.set = true
	return nil
}

// setDefaults sets default ticket paths for the flag. These values are cleared
// the first time the flag is explicitly parsed in the flag set.
func (tf *TicketFlags) setDefaults(tickets []string) {
	tf.Tickets = tickets
	tf.dedup = map[string]bool{}
	for _, t := range tickets {
		tf.dedup[t] = true
	}
	tf.fs.Lookup(tf.ticketFlag).DefValue = strings.Join(tickets, ",")
}

// String implements flag.Value.
func (tf *TicketFlags) String() string {
	return strings.Join(tf.Tickets, ",")
}

// ReadEnvOrFile will attempt to obtain values for the tickets to use from
// the environment or from a file if none have been explicitly set on the
// command line. If no flags were specified it will read the environment
// variable GRAIL_TICKETS and if that's empty it will attempt to read the
// file specified by the ticketrc flag (or it's default value).
func (tf *TicketFlags) ReadEnvOrFile() error {
	if tf.set {
		return nil
	}
	if te := os.Getenv("GRAIL_TICKETS"); len(te) > 0 {
		return tf.Set(te)
	}
	f, err := os.Open(tf.TicketRCFile)
	if err != nil {
		// It's ok for the rc file to not exist if it hasn't been set.
		if tf.ticketRCFlag.set && !os.IsExist(err) {
			return err
		}
		return nil
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		if l := strings.TrimSpace(sc.Text()); len(l) > 0 {
			tf.Set(l)
		}
	}
	return sc.Err()
}

// RegisterTicketFlags registers the ticket related flags with the
// supplied FlagSet. The flags are:
// --<prefix>ticket
// --<prefix>ticket-timeout
// --<prefix>ticketrc
func RegisterTicketFlags(fs *flag.FlagSet, prefix string, defaultTickets []string, flags *TicketFlags) {
	flags.fs = fs
	desc := "Comma separated list of GRAIL security tickets, and/or the flag may be repeated"
	fs.Var(flags, prefix+"ticket", desc)
	fs.DurationVar(&flags.Timeout, prefix+"ticket-timeout", time.Minute, "specifies the timeout duration for obtaining any single GRAIL security ticket")
	flags.ticketRCFlag.name = prefix + "ticketrc"
	flags.ticketRCFlag.val = &flags.TicketRCFile
	flags.TicketRCFile = filepath.Join(os.Getenv("HOME"), ".ticketrc")
	fs.Var(&flags.ticketRCFlag, flags.ticketRCFlag.name, "a file containing the tickets to use")
	fs.Lookup(prefix + "ticketrc").DefValue = "$HOME/.ticketrc"
	flags.ticketFlag = prefix + "ticket"
	flags.rcFlag = prefix + "ticketrc"
	flags.setDefaults(defaultTickets)
}
