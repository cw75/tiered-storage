if [[ -z $1 ]]; then
  TYPE=Release
else
  TYPE=$1
fi

rm -rf build
mkdir build
cd build
cmake -std=c++11 -DCMAKE_BUILD_TYPE=$TYPE ..
make