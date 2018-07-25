// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package passwd

import (
	"crypto/sha256"
	"crypto/subtle"
	"fmt"
	"syscall"

	"golang.org/x/crypto/bcrypt"
	"golang.org/x/crypto/ssh/terminal"
)

// ReadAndHashPassword reads a password from stdin, taking care to not echo
// it and then immediately hashes the password using an expensive (bcrypt)
// hash function. This hash may be safely stored to disk. It also returns a
// second, stable, hash of the password that may be used for symmetric
// encryption of data. It returns the cost and salt used to generate the hash
// since they need to be available when verifying a password against the hash.
func ReadAndHashPassword() (hash []byte, key AESKey, salt []byte, cost, major, minor int, err error) {
	fmt.Print("Enter Password: ")
	password, err := terminal.ReadPassword(int(syscall.Stdin))
	fmt.Println("")
	defer func() {
		// Zero out the password as soon as it's hashed.
		for i := range password {
			password[i] = 0
		}

	}()

	if err != nil {
		return
	}
	return hashPassword(password)
}

func hashPassword(password []byte) (hash []byte, key AESKey, salt []byte, cost, major, minor int, err error) {
	salt, err = bcrypt.GenerateSalt()
	if err != nil {
		return
	}

	cost = bcrypt.DefaultCost
	bkey, err := bcrypt.Bcrypt(password, salt, cost)
	if err != nil {
		return
	}

	if n := copy(key[:], bkey); n < cap(key) {
		err = fmt.Errorf("key too short")
		return
	}
	sum := sha256.Sum256(key[:])
	hash = sum[:]
	major = bcrypt.MajorVersion
	minor = bcrypt.MinorVersion
	return
}

// ReadAndComparePassword reads a password from stdin, taking care to not echo
// it and then compares that password with the supplied hash.
func ReadAndComparePassword(hash, salt []byte, cost, major, minor int) (AESKey, error) {
	fmt.Print("Enter Password: ")
	password, err := terminal.ReadPassword(int(syscall.Stdin))
	fmt.Println("")
	defer func() {
		// Zero out the password as soon as it's hashed.
		for i := range password {
			password[i] = 0
		}

	}()
	if err != nil {
		return zeroKey, err
	}
	return comparePassword(password, hash, salt, cost, major, minor)
}

func comparePassword(password, hash, salt []byte, cost, major, minor int) (AESKey, error) {
	bkey, err := bcrypt.Bcrypt(password, salt, cost)
	if err != nil {
		return zeroKey, err
	}
	key := zeroKey
	if n := copy(key[:], bkey); n < cap(key) {
		return zeroKey, fmt.Errorf("key too short")
	}
	h := sha256.Sum256(key[:])
	if subtle.ConstantTimeCompare(h[:], hash) == 0 {
		return zeroKey, fmt.Errorf("mismatched passwords")
	}
	return key, err
}
