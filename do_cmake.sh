#!/bin/sh -x

MODE=Debug
#MODE=Release

git submodule update --init --recursive
if test -e build; then
    echo 'build dir already exists; rm -rf build and re-run'
    exit 1
fi

ARGS=""
if which ccache ; then
    echo "enabling ccache"
    ARGS="${ARGS} -DWITH_CCACHE=ON"
fi
ARGS="${ARGS} -DCMAKE_BUILD_TYPE=${MODE}"
#ARGS="${ARGS} -DALLOCATOR=jemalloc"
ARGS="${ARGS} -DWITH_TESTS=OFF"

mkdir build
cd build
cmake -DBOOST_J=$(nproc) $ARGS "$@" ..

# minimal config to find plugins
cat <<EOF > ceph.conf
plugin dir = lib
erasure code dir = lib
EOF

# give vstart a (hopefully) unique mon port to start with
echo $(( RANDOM % 1000 + 40000 )) > .ceph_port

echo done.
