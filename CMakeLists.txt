cmake_minimum_required(VERSION 3.17)
set(CMAKE_TOOLCHAIN_FILE $ENV{HOME}/vcpkg/scripts/buildsystems/vcpkg.cmake CACHE STRING "Vcpkg toolchain file")
project(PandasCPP)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(Boost REQUIRED COMPONENTS filesystem system date_time)
find_package(tabulate CONFIG REQUIRED)
# install arrow from here https://arrow.apache.org/install/

add_library(pandas_arrow series.cpp scalar.cpp dataframe.cpp core.cpp)

target_link_libraries(pandas_arrow PRIVATE arrow parquet ${Boost_LIBRARIES}  tabulate::tabulate)
target_include_directories(pandas_arrow PUBLIC . ${Boost_INCLUDE_DIRS})

add_subdirectory(arrow)
add_subdirectory(test)