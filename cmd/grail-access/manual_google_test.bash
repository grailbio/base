#!/bin/bash

# The grail-access Google authentication flow is difficult to exercise in a unit test because the
# outh2 server isn't trivial to fake (at least not for josh@).

# Instead, we provide a manual test script. Please run this yourself.

set -euo pipefail

echo "Instructions:"
echo
echo "Running manual tests. If the script exits with a non-zero error code, the tests failed."
echo "You'll also be prompted to review output manually. If it doesn't look right, the tests failed."
echo
dir="$(mktemp -d)"
echo "Using temporary directory for test: $dir"
echo "Building grail-access for the test..."
cd "$( dirname "${BASH_SOURCE[0]}" )"
go build -o "$dir/grail-access" github.com/grailbio/base/cmd/grail-access
cd "$dir"
echo
echo "Step 1/3: Starting grail-access Google authentication flow. Please complete it."
echo
echo "************************************************************************"
./grail-access -dir ./v23
echo "************************************************************************"
echo
echo "Done with authentication flow."
echo "If it succeeded, you should lines like these above:"
echo "    Default Blessings      v23.grail.com:google:YOUR_USERNAME@grailbio.com"
echo "and"
echo "    ...                    v23.grail.com:google:YOUR_USERNAME@grailbio.com"
echo "and an expiration date in the future."
echo
read -p "Continue with next test? [Y] "
echo
echo "Step 2/3: Next, running the same flow, but automatically canceling."
echo
echo "************************************************************************"
set +e
cat /dev/null | ./grail-access -dir ./v23 -browser=false
set -e
echo "************************************************************************"
echo
echo "Step 3/3: Finally, make sure our Step 1 credentials survived. "
echo
echo "************************************************************************"
./grail-access -dir ./v23 -dump
echo "************************************************************************"
echo
echo "You should see the same blessing lines as in Step 1, and a consistent expiry time."
echo "If not, the tests failed."
echo
echo "Cleaning up test directory: $dir"
rm -rf "$dir"
