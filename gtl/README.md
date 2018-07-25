# Poorman's Go templates

This directory contains algorithms written using a pidgin templates.

# Directory contents

- rcu_map: concurrent hash map. Readers can access the map without memory
  barriers.

- unsafe: unsafe, but efficient slice operations, including casting between
  string and []byte and uninitialized slice resizing.

- freepool: freepool for a concrete type. It is similar to sync.Pool, but it is
  specialized for a particular type, and it relies on external mutex for thread
  safety.

- randomized_freepool: thread safe freepool for a concrete type. It uses a
  power-of-two loadbalancing to balance pools with from other CPUs, so it scales
  better than sync.Pool on many-core machines. However, unlike sync.Pool, it
  never releases idle objects.

# Example

Directory tests/ contains files instantiated from the templates. Grep for
"//go:generate" lines in this directory for the commandlines used to generate
these files.
