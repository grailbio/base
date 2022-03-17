#!/usr/bin/env python3.6

import argparse
import logging
import os
import re
import subprocess
import sys
from typing import List


def main() -> None:
    logging.basicConfig(level=logging.DEBUG)
    output = None
    package = None
    generate_argv: List[str] = []
    for arg in sys.argv[1:]:
        m = re.match("^--package=(.*)", arg)
        if m:
            package = m[1]
        m = re.match("^--output=(.*)", arg)
        if m:
            output = m[1]
        else:
            generate_argv.append(arg)

    if not output or not package:
        raise Exception("--output and --package not set")

    gtl_dir = os.path.dirname(sys.argv[0])
    output_dir = os.path.dirname(output)
    if not output_dir:
        output_dir = "."

    cmdline = (
        [sys.executable, os.path.join(gtl_dir, "generate.py"), f"--output={output}.go"]
        + generate_argv
        + [os.path.join(gtl_dir, "randomized_freepool.go.tpl")]
    )
    logging.debug("CMDLINE %s", cmdline)
    subprocess.check_call(
        [sys.executable, os.path.join(gtl_dir, "generate.py"), f"--output={output}.go"]
        + generate_argv
        + [os.path.join(gtl_dir, "randomized_freepool.go.tpl")]
    )
    subprocess.check_call(
        [
            sys.executable,
            os.path.join(gtl_dir, "generate.py"),
            f"--output={output}_race.go",
        ]
        + generate_argv
        + [os.path.join(gtl_dir, "randomized_freepool_race.go.tpl")]
    )

    with open(f"{output_dir}/randomized_freepool_internal.s", "w") as fd:
        fd.write(
            """// Code generated by generate_randomized_freepool.py. DO NOT EDIT.

// Dummy file to force the go compiler to honor go:linkname directives. See
//
// https://github.com/golang/go/issues/15006
// http://www.alangpierce.com/blog/2016/03/17/adventures-in-go-accessing-unexported-functions/
"""
        )

    with open(f"{output_dir}/randomized_freepool_internal.go", "w") as fd:
        fd.write(
            f"""// Code generated by generate_randomized_freepool.py. DO NOT EDIT.
package {package}

// This import is needed to use go:linkname.
import _ "unsafe"

// The following functions are defined in go runtime.  To use them, we need to
// import "unsafe", and elsewhere in this package, import "C" to force compiler
// to recognize the "go:linktime" directive. Some of the details are explained
// in the below blog post.
//
// procPin() pins the caller to the current processor, and returns the processor
// id in range [0,GOMAXPROCS). procUnpin() undos the effect of procPin().
//
// http://www.alangpierce.com/blog/2016/03/17/adventures-in-go-accessing-unexported-functions/

//go:linkname runtime_procPin sync.runtime_procPin
//go:nosplit
func runtime_procPin() int

//go:linkname runtime_procUnpin sync.runtime_procUnpin
//go:nosplit
func runtime_procUnpin()

//go:linkname fastrandn sync.fastrandn
func fastrandn(n uint32) uint32
"""
        )


main()
