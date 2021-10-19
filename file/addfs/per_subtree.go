package addfs

// TODO: Implement PerSubtreeFunc.
// A PerNodeFunc is applied independently to each node in an entire directory tree. It may be
// useful to define funcs that are contextual. For example if an fsnode.Parent called base/ has a
// child called .git, we may want to define git-repository-aware views for each descendent node,
// like base/file/addfs/.../per_subtree.go/git/log.txt containing history.
