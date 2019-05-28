#!/bin/sh

set -e
set -x

# Run python bindings test
$(find . -name test.py)

# Run cpp bindings test
$(find . -name eblob_cpp_test)

# Run unit tests
$(find . -name eblob_crypto_test)
$(find . -name eblob_corruption_test)
$(find . -name eblob_defrag_test)

# Big and small stress tests
$(find . -name eblob_stress) -m0 -f1000 -D0 -I300000 -o20000 -i1000 -l4 -r 1000 -S10 -F87
$(find . -name eblob_stress) -m0 -f100 -D0 -I30000 -o2000 -i100 -l4 -r 100 -S100 -F14
$(find . -name eblob_stress) -m0 -f1000 -D0 -I300000 -o20000 -i1000 -l4 -r 1000 -S10 -F2135
$(find . -name eblob_stress) -m0 -f100 -D0 -I30000 -o2000 -i100 -l4 -r 100 -S100 -F2062

# Big and small stress tests with views over bases
$(find . -name eblob_stress) -m0 -f1000 -D0 -I300000 -o20000 -i1000 -l4 -r 1000 -S10 -F4183
$(find . -name eblob_stress) -m0 -f100 -D0 -I30000 -o2000 -i100 -l4 -r 100 -S100 -F4110
$(find . -name eblob_stress) -m0 -f1000 -D0 -I300000 -o20000 -i1000 -l4 -r 1000 -S10 -F6231
$(find . -name eblob_stress) -m0 -f100 -D0 -I30000 -o2000 -i100 -l4 -r 100 -S100 -F6158

# Use specific datasort_dir for sorting chunks
$(find . -name eblob_stress) -m0 -f1000 -D0 -I300000 -o20000 -i1000 -l4 -r 1000 -S10 -F2263 -P 1

# Overwrite-heavy test with many bases and threads
$(find . -name eblob_stress) -f1000 -D0 -I100000 -i64 -r 40 -S10 -F64 -T32 -l4 -o 0
$(find . -name eblob_stress) -f1000 -D0 -I100000 -i64 -r 40 -S10 -F2112 -T32 -l4 -o 0
