#! /bin/bash

# Some useful links:
# - http://clang.llvm.org/get_started.html
# - http://apt.llvm.org/
# - http://askubuntu.com/questions/787383/how-to-install-llvm-3-9
# - https://omtcyfz.github.io/2016/08/30/Improving-workflow-by-using-Clang-based-tools.html

set -euo pipefail

main() {
    sudo apt-get install -y \
        clang-3.8 clang-format-3.8 clang-tidy-3.8 libc++-dev libc++abi-dev
    sudo ln -s "$(which clang-3.8)" /usr/bin/clang
    sudo ln -s "$(which clang++-3.8)" /usr/bin/clang++
    sudo ln -s "$(which clang-format-3.8)" /usr/bin/clang-format
    sudo ln -s "$(which clang-tidy-3.8)" /usr/bin/clang-tidy

    # TODO(mwhittaker): Install clang-include-fixer, asan, tsan, and msan.
}

main
