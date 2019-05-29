include_directories(../include)

# stress
add_executable(eblob_stress stress/stress.c stress/options.c)
target_link_libraries(eblob_stress eblob)

# cpp bindings
set(EBLOB_CPP_TEST_SRCS cpp/test.cpp)
add_executable(eblob_cpp_test ${EBLOB_CPP_TEST_SRCS})
target_link_libraries(eblob_cpp_test ${EBLOB_CPP_LIBRARIES} eblob_cpp eblob)
add_custom_target(test_cpp
                  COMMAND "${CMAKE_CURRENT_BINARY_DIR}/eblob_cpp_test"
                  DEPENDS ${TESTS_DEPS} eblob_cpp_test)

# unit tests
add_executable(eblob_crypto_test unit/crypto.cpp)
target_link_libraries(eblob_crypto_test eblob ${Boost_LIBRARIES})
add_custom_target(test_crypto
                  COMMAND "${CMAKE_CURRENT_BINARY_DIR}/eblob_crypto_test"
                  DEPENDS ${TESTS_DEPS} eblob_crypto_test)

add_executable(eblob_corruption_test unit/corruption.cpp unit/eblob_wrapper.cpp)
target_link_libraries(eblob_corruption_test eblob ${Boost_LIBRARIES})
add_custom_target(test_corruption
                  COMMAND "${CMAKE_CURRENT_BINARY_DIR}/eblob_corruption_test"
                  DEPENDS ${TESTS_DEPS} eblob_corruption_test)

add_executable(eblob_defrag_test unit/defrag.cpp unit/eblob_wrapper.cpp)
target_link_libraries(eblob_defrag_test eblob ${Boost_LIBRARIES})
add_custom_target(test_defrag
                  COMMAND "${CMAKE_CURRENT_BINARY_DIR}/eblob_defrag_test"
                  DEPENDS ${TESTS_DEPS} eblob_defrag_test)

set(TESTS_LIST
    eblob_stress
    eblob_cpp_test
    eblob_crypto_test
    eblob_defrag_test
    eblob_corruption_test
)
set(TESTS_DEPS ${TESTS_LIST})

add_custom_target(test
                  COMMAND "${CMAKE_CURRENT_SOURCE_DIR}/run_tests.sh"
                  DEPENDS ${TESTS_DEPS})
