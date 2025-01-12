find_package(Catch2 REQUIRED)

add_executable(pandasTest
        concat_test.cpp
        core_test.cpp
        dataframe_aggregation_test.cpp
        dataframe_arithmetric_test.cpp
        dataframe_indexing_test.cpp
        dataframe_iterator_test.cpp
        dataframe_test.cpp
        scalar_test.cpp
        series_aggregation_test.cpp
        series_arithmetric_test.cpp
        series_indexing_test.cpp
        series_iterator_test.cpp
        series_resample_test.cpp
        series_test.cpp)
target_include_directories(pandasTest PRIVATE ../..)
target_link_libraries(pandasTest PRIVATE Catch2::Catch2WithMain pandas_arrow)

include(CTest)
include(Catch)
catch_discover_tests(pandasTest)

add_executable(benchmark_ benchmark.cpp )
target_include_directories(benchmark_ PRIVATE ../..)
target_link_libraries(benchmark_ PRIVATE pandas_arrow )


add_subdirectory(cudf_examples)
