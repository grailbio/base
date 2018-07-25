# grail-file

Command `grail-file` provides common file operations using https://godoc.org/github.com/grailbio/base/file

`grail-file --help` will show full help.

Examples:

    grail-file cp s3://bucket0/key0 s3://bucket1/key1
    grail-file cp /tmp/somefile s3://bucket1/key2
    grail-file ls s3://bucket0/key0
    grail-file cat s3://bucket0/key0/file
