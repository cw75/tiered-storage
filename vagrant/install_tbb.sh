#! /bin/bash

set -euo pipefail

main() {
    # TODO(mwhittaker): Understand libstdc++ vs libc++ and figure out if this
    # is okay.
    sudo apt-get install -y libtbb-dev
}

main
