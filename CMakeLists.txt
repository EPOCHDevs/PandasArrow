cmake_minimum_required(VERSION 3.17)
set(CMAKE_TOOLCHAIN_FILE $ENV{HOME}/vcpkg/scripts/buildsystems/vcpkg.cmake CACHE STRING "Vcpkg toolchain file")

project(PandasCPP)

option(BUILD_TEST OFF)
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
option(Boost_USE_STATIC_LIBS  ON)

ADD_SUBDIRECTORY(third_party)

find_package(Boost REQUIRED COMPONENTS filesystem system date_time )
find_package(tabulate CONFIG REQUIRED)
find_package(TBB CONFIG REQUIRED)

add_library(pandas_arrow series.cpp scalar.cpp
        dataframe.cpp core.cpp resample.cpp concat.cpp)

target_link_libraries(pandas_arrow PRIVATE
        ${Boost_LIBRARIES}
        TBB::tbb tabulate::tabulate
        arrow_shared parquet_shared arrow_cuda_shared)

target_include_directories(pandas_arrow PUBLIC . ${Boost_INCLUDE_DIRS}
        third_party/installed/arrow/include
)
target_compile_options(pandas_arrow PRIVATE -Werror)

# installation is only required for header files
add_dependencies(pandas_arrow install_arrow)

IF( BUILD_TEST)
    add_subdirectory(test)
ENDIF()