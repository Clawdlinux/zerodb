#!/usr/bin/env bash
# Builds LazyFS at a pinned commit into $1, with one addition: statfs.
# Without statfs, FUSE reports 0 free bytes, so zerodb's disk-full floor makes it read-only.
# Needs g++, cmake, libfuse3-dev and git.
set -euo pipefail
DIR=${1:?usage: build-lazyfs.sh <dir>}
REF=${LAZYFS_REF:-fa7d32ee9710d55a7e529ac3c375f6616563c78b}
[ -d "$DIR/.git" ] || git clone -q https://github.com/dsrhaslab/lazyfs "$DIR"
cd "$DIR"
git checkout -q --detach "$REF"
git checkout -q -- .
sed -i 's|^\( *\)static int lfs_access (const char\* path, int mask);|&\n\1static int lfs_statfs (const char* path, struct statvfs* stbuf);|' \
    lazyfs/include/lazyfs/lazyfs.hpp
grep -q lfs_statfs lazyfs/include/lazyfs/lazyfs.hpp
cat >>lazyfs/src/lazyfs.cpp <<'EOF'

#include <sys/statvfs.h>

// Added by zerodb's build-lazyfs.sh: the backing filesystem's space.
int lazyfs::LazyFS::lfs_statfs (const char* path, struct statvfs* stbuf) {
    return statvfs (path, stbuf) == -1 ? -errno : 0;
}
EOF
(cd libs/libpcache && ./build.sh >/dev/null)
(cd lazyfs && ./build.sh >/dev/null)
test -x lazyfs/build/lazyfs
