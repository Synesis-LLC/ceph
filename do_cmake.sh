#!/bin/sh -x
git submodule update --init --recursive

BUILD_DIR=build_debug
MODE=Debug # Release

if test -e $BUILD_DIR; then
    echo $BUILD_DIR' dir already exists; rm -rf '$BUILD_DIR' and re-run'
    exit 1
fi
mkdir $BUILD_DIR
cd $BUILD_DIR
cmake -DBOOST_J=$(nproc) -DCMAKE_BUILD_TYPE=$MODE -DALLOCATOR=jemalloc -DWITH_TESTS=OFF "$@"  ..

# minimal config to find plugins
cat <<EOF > ceph.conf
plugin dir = lib
erasure code dir = lib
EOF

# give vstart a (hopefully) unique mon port to start with
echo $(( RANDOM % 1000 + 40000 )) > .ceph_port

echo done.
