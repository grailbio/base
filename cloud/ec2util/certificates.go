// Copyright 2018 GRAIL, Inc. All rights reserved.
// Use of this source code is governed by the Apache-2.0
// license that can be found in the LICENSE file.

package ec2util

// awsPublicCertificatePEM is the certificate used to verify the integrity of
// the EC2 instance identity documents. If this certificate os out-of-date
// then identityd-aws will refuse valid blessing requests.
//
// TODO(razvanm): add an integration test to catch if real identity documents
// are not signed with this certificate anymore.
//
// Reference: http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/instance-identity-documents.html
//
// Output from `openssl x509 -in /tmp/cert -text -noout` is the following:
//
// Certificate:
//     Data:
//         Version: 1 (0x0)
//         Serial Number:
//             96:ba:48:d9:e5:5e:1a:67
//     Signature Algorithm: dsaWithSHA1
//         Issuer: C=US, ST=Washington State, L=Seattle, O=Amazon Web Services LLC
//         Validity
//             Not Before: Jan  5 12:56:12 2012 GMT
//             Not After : Jan  5 12:56:12 2038 GMT
//         Subject: C=US, ST=Washington State, L=Seattle, O=Amazon Web Services LLC
//         Subject Public Key Info:
//             Public Key Algorithm: dsaEncryption
//                 pub:
//                     46:e6:7a:f7:b9:7f:c2:c8:13:f1:9f:30:d9:8f:f4:
//                     23:39:7a:8b:d0:38:6c:79:86:8f:16:a8:3f:9a:4d:
//                     e6:ec:fa:4d:ad:9f:dd:81:04:fa:9e:66:aa:56:45:
//                     ae:4f:ff:e8:eb:74:a9:a5:d6:ac:78:96:51:d6:31:
//                     7a:ec:dc:88:08:6f:af:a1:50:80:7e:4b:c7:73:f3:
//                     f9:a6:3b:46:e9:af:be:91:a1:95:42:5d:86:9b:d8:
//                     26:5e:74:5b:ad:ee:a7:37:59:41:fd:f8:45:00:1f:
//                     99:b0:bd:27:83:08:e5:0c:61:04:7b:47:d2:c8:35:
//                     8c:28:b8:72:33:8a:a4:18
//                 P:
//                     00:a3:92:f7:12:d9:b6:f5:55:0e:32:b7:fe:5e:8a:
//                     1e:4e:3b:a9:0a:fe:7d:4b:ce:59:6b:ec:3f:19:c2:
//                     d4:0f:f1:f3:84:a6:9e:44:da:78:3e:0f:a5:1d:d5:
//                     9d:60:62:2a:6b:e7:c2:a3:de:7b:2c:48:da:48:e9:
//                     b5:f7:57:22:10:cc:ae:f0:2d:c9:67:77:f6:28:ce:
//                     a3:4d:9a:02:32:65:e7:0d:ec:48:25:2b:d2:13:1c:
//                     92:cf:fb:1f:05:b5:4a:6d:dd:06:f2:61:72:5d:99:
//                     53:1e:80:de:8f:86:f8:98:b7:92:47:fe:76:25:e2:
//                     18:77:3d:ad:47:25:db:24:81
//                 Q:
//                     00:b5:49:dc:44:75:d7:e2:a8:e2:d3:fa:9f:0b:c7:
//                     c2:1e:be:11:16:11
//                 G:
//                     00:8d:63:93:eb:64:a8:c5:47:b8:01:5c:bc:01:8a:
//                     a1:c4:e0:b2:32:5e:9b:af:f9:aa:89:b3:26:e2:83:
//                     99:b2:4d:bb:d4:31:c3:7a:c0:a2:d5:da:bb:c4:1e:
//                     f8:c0:5c:26:5f:d4:86:1c:b5:28:75:69:08:69:7d:
//                     7e:e7:90:da:ce:88:2f:40:9a:3b:f6:ee:c9:c5:35:
//                     0a:26:10:93:8d:0a:34:70:9c:1d:f3:62:65:b7:65:
//                     c7:51:3d:ae:95:29:ed:b9:c5:95:16:b6:bf:59:ed:
//                     31:7d:dc:69:15:43:75:bb:4f:11:69:9d:c5:20:98:
//                     b1:81:43:93:89:99:ac:e2:c1
//     Signature Algorithm: dsaWithSHA1
//          r:
//              59:70:65:93:8d:31:4f:04:b0:ec:75:f7:d8:cc:57:
//              62:bb:ac:7b:d0
//          s:
//              13:46:99:d5:f6:64:1a:d5:34:6a:cd:f4:dd:9f:e9:
//              13:a4:d2:4f:4a
const awsPublicCertificatePEM = `-----BEGIN CERTIFICATE-----
MIIC7TCCAq0CCQCWukjZ5V4aZzAJBgcqhkjOOAQDMFwxCzAJBgNVBAYTAlVTMRkw
FwYDVQQIExBXYXNoaW5ndG9uIFN0YXRlMRAwDgYDVQQHEwdTZWF0dGxlMSAwHgYD
VQQKExdBbWF6b24gV2ViIFNlcnZpY2VzIExMQzAeFw0xMjAxMDUxMjU2MTJaFw0z
ODAxMDUxMjU2MTJaMFwxCzAJBgNVBAYTAlVTMRkwFwYDVQQIExBXYXNoaW5ndG9u
IFN0YXRlMRAwDgYDVQQHEwdTZWF0dGxlMSAwHgYDVQQKExdBbWF6b24gV2ViIFNl
cnZpY2VzIExMQzCCAbcwggEsBgcqhkjOOAQBMIIBHwKBgQCjkvcS2bb1VQ4yt/5e
ih5OO6kK/n1Lzllr7D8ZwtQP8fOEpp5E2ng+D6Ud1Z1gYipr58Kj3nssSNpI6bX3
VyIQzK7wLclnd/YozqNNmgIyZecN7EglK9ITHJLP+x8FtUpt3QbyYXJdmVMegN6P
hviYt5JH/nYl4hh3Pa1HJdskgQIVALVJ3ER11+Ko4tP6nwvHwh6+ERYRAoGBAI1j
k+tkqMVHuAFcvAGKocTgsjJem6/5qomzJuKDmbJNu9Qxw3rAotXau8Qe+MBcJl/U
hhy1KHVpCGl9fueQ2s6IL0CaO/buycU1CiYQk40KNHCcHfNiZbdlx1E9rpUp7bnF
lRa2v1ntMX3caRVDdbtPEWmdxSCYsYFDk4mZrOLBA4GEAAKBgEbmeve5f8LIE/Gf
MNmP9CM5eovQOGx5ho8WqD+aTebs+k2tn92BBPqeZqpWRa5P/+jrdKml1qx4llHW
MXrs3IgIb6+hUIB+S8dz8/mmO0bpr76RoZVCXYab2CZedFut7qc3WUH9+EUAH5mw
vSeDCOUMYQR7R9LINYwouHIziqQYMAkGByqGSM44BAMDLwAwLAIUWXBlk40xTwSw
7HX32MxXYruse9ACFBNGmdX2ZBrVNGrN9N2f6ROk0k9K
-----END CERTIFICATE-----`
