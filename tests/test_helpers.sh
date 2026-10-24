#!/usr/bin/env bash
# Shared assertions for the installer / deploy / supply-chain fixture scripts.
#
# Sourced, never executed: every helper aborts the calling script on failure so
# a broken fixture cannot silently pass. The definition used to be copied into
# each tests/*.sh file.

fail_test() {
    echo "FAIL: $*" >&2
    exit 1
}

assert_line() {
    local file="$1"
    local expected="$2"
    grep -Fqx -- "${expected}" "${file}" \
        || fail_test "missing expected line in ${file}: ${expected}"
}
