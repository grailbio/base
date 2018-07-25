//go:build bazel

package filebench

import _ "embed"

//go:embed s3fuse_binary
var s3FUSEBinary []byte
