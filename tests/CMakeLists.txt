find_package(Catch2 REQUIRED)

add_executable(pandasTest core_test.cpp scalar_test.cpp  series_test.cpp dataframe_test.cpp downsample_test.cpp)
target_include_directories(pandasTest PRIVATE ../..)
target_link_libraries(pandasTest PRIVATE Catch2::Catch2WithMain pandas_arrow)

include(CTest)
include(Catch)
catch_discover_tests(pandasTest)

add_executable(benchmark_ benchmark.cpp )
target_include_directories(benchmark_ PRIVATE ../..)
target_link_libraries(benchmark_ PRIVATE pandas_arrow )

add_executable(hash_practise hash_practise.cpp )
target_include_directories(hash_practise PRIVATE ../..)
target_link_libraries(hash_practise PRIVATE pandas_arrow )

add_subdirectory(cudf_examples)
