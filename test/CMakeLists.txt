find_package(Catch2 REQUIRED)

add_executable(series_test series_test.cpp)
target_include_directories(series_test PRIVATE ../..)
target_link_libraries(series_test PRIVATE Catch2::Catch2WithMain pandas_arrow)

add_executable(dataframe_test dataframe_test.cpp )
target_include_directories(dataframe_test PRIVATE ../..)
target_link_libraries(dataframe_test PRIVATE Catch2::Catch2WithMain pandas_arrow)

add_executable(core_test core_test.cpp )
target_include_directories(core_test PRIVATE ../..)
target_link_libraries(core_test PRIVATE Catch2::Catch2WithMain  pandas_arrow )

add_executable(scalar_test scalar_test.cpp )
target_include_directories(scalar_test PRIVATE ../..)
target_link_libraries(scalar_test PRIVATE Catch2::Catch2WithMain  pandas_arrow )

add_executable(custom_kernel_test custom_kernel_test.cpp )
target_include_directories(custom_kernel_test PRIVATE ../..)
target_link_libraries(custom_kernel_test PRIVATE Catch2::Catch2WithMain  pandas_arrow )
