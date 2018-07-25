// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// Package passwd provides an interactive password based key registry for use
// with github.com/grailbio/crypto/encryption.
//
// It uses bcrypt to generate an encryption key and sha256 of that key to
// generate the password hash. Thus bcrypt is required for any brute force
// attacks on both the key and the hash.
//
// The bcrypt hash (23 bytes) is used as the encryption key, or more precisely,
// the lower 16 bytes are used as an AES128 key. The sha256 of the full 23
// bytes is used the password 'hash' and is stored in the encrypted file
// along with metadata such as the salt, bcrypt cost etc.
//
// Thus, the full set of operations in outline is:
//
// encryption/write side:
//
// key, metadata = bcrypt(password)
// hash = sha256(key)
//
// file = hash, aes128(key, plaintext)
//
// decryption/read side:
// read hash and metadata from the file
// key = bcrypt(password, metadata)
// newhash = sha256(key)
// if newhash == hash {
//     key is valid
// }
//
// The ReadAndHashPassword method implements the 'write' side and
// ReadAndComparePassword the 'read' side. This routines take care to read the
// password safely from a terminal and to keep the password in memory for as
// short a time as possible. The encryption key however is kept in memory.
package passwd
