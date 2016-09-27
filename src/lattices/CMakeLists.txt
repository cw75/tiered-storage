CMAKE_MINIMUM_REQUIRED(VERSION 2.6)

# CREATE_TEST(foo) creates a test named `foo` from the file `foo.cc`.
MACRO(CREATE_TEST NAME)
    ADD_EXECUTABLE(${NAME} ${NAME}.cc)
    ADD_TEST(${NAME} ${NAME})
    ADD_DEPENDENCIES(${NAME} googletest)
    TARGET_LINK_LIBRARIES(${NAME}
        ${GTEST_LIBS_DIR}/libgtest.a
        ${GTEST_LIBS_DIR}/libgtest_main.a
    )
ENDMACRO(CREATE_TEST)

CREATE_TEST(array_lattice_test)
CREATE_TEST(bool_and_lattice_test)
CREATE_TEST(bool_or_lattice_test)
CREATE_TEST(map_lattice_test)
CREATE_TEST(max_lattice_test)
CREATE_TEST(min_lattice_test)
CREATE_TEST(pair_lattice_test)
CREATE_TEST(set_intersect_lattice_test)
CREATE_TEST(set_union_lattice_test)
CREATE_TEST(timestamp_lattice_test)
CREATE_TEST(vector_lattice_test)
