rm -rf vendor/gtest/build
mkdir vendor/gtest/build
cd vendor/gtest/build
cmake -DBUILD_SHARED_LIBS=ON ..
make