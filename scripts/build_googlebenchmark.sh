rm -rf vendor/gbenchmark/build
mkdir vendor/gbenchmark/build
cd vendor/gbenchmark/build
cmake -DBUILD_SHARED_LIBS=ON -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_COMPILER="clang++" -DCMAKE_CXX_FLAGS="-std=c++11 -stdlib=libc++" ..
make
