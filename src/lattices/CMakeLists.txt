CMAKE_MINIMUM_REQUIRED(VERSION 2.6)

ADD_EXECUTABLE(bool_or_lattice_test bool_or_lattice_test.cc)
ADD_TEST(bool_or_lattice_test bool_or_lattice_test)
ADD_DEPENDENCIES(bool_or_lattice_test googletest)
TARGET_LINK_LIBRARIES(bool_or_lattice_test
    ${GTEST_LIBS_DIR}/libgtest.a
    ${GTEST_LIBS_DIR}/libgtest_main.a
)
