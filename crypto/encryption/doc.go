// Copyright 2017 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

// Package encryption provides support for encrypting and decrypting data
// and files with granular key management.
//
// It is assumed that the data being encrypted is archival and long lived.
//
// The key management scheme supports arbitrary ciphers and many keys,
// the intent being to easily support using different keys per file.
// The encryption algorithm, blocksize, HMAC are determined by the choice
// of key management scheme.
//
// The encryption interface supports both traditional block-based and AEAD
// APIs.
//
// Encrypted files are layered on top of the encoding/recordio format
// whereby the first record in the file is used to store a header containing
// the necessary metadata to decrypt the remaining records in the file. For
// such files a single key is used to encrypt all of the data within the file.
// The recordio format encrypts each record as an independent block with its
// own encryption metadata (eg. IV, HMAC) and hence is not suitable for use
// with lots of small records due to the space overhead of this metadata.
//
// The format of the header record is:
// crc32 of the marshalled Key Descriptor JSON record.
// JSON encoding of KeyDescriptor
//
// The format of each encrypted record is:
// Initialization Vector (IV)
// encrypted(HMAC(plaintext) + plaintext)
package encryption
