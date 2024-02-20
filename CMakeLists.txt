cmake_minimum_required(VERSION 3.27 FATAL_ERROR)

set(CMAKE_TOOLCHAIN_FILE $ENV{HOME}/vcpkg/scripts/buildsystems/vcpkg.cmake CACHE STRING "Vcpkg toolchain file")
set(CMAKE_CUDA_COMPILER "/usr/local/cuda/bin/nvcc"  CACHE PATH "CUDA COMPILER")
set(SAM_RESEARCH "$ENV{HOME}/SAMResearch"  CACHE PATH "SAM RESEARCH FOLDER")

########################################### INCLUDE RAPIDS CMAKE FILES #################################################
include(cmake/fetch_rapids.cmake)

include(rapids-cmake)
include(rapids-cpm)
include(rapids-cuda)
include(${rapids-cmake-dir}/cuda/set_runtime.cmake)
include(rapids-export)
include(rapids-find)
########################################################################################################################

rapids_cuda_init_architectures(PandasArrow)
project(PandasArrow
        LANGUAGES C CXX CUDA)

option(BUILD_TEST OFF)
set(Boost_NO_WARN_NEW_VERSIONS ON)
option(Boost_USE_STATIC_LIBS  ON)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# add third party dependencies
rapids_cuda_init_runtime(USE_STATIC ON)
rapids_cpm_init()

include(cmake/get_cudf.cmake)
include(cmake/dependencies.cmake)

add_library(pandas_arrow
        src/series.cpp
        src/scalar.cpp
        src/dataframe.cpp
        src/core.cpp
        src/resample.cpp
        src/concat.cpp
        src/cudf/cudf_utils.cpp
        src/cudf/dataframe.cpp
        src/cudf/scalar.cpp
        src/cudf/series.cpp)

target_link_libraries(pandas_arrow PRIVATE
        ${Boost_LIBRARIES} tabulate::tabulate
        PUBLIC TBB::tbb nvcomp::nvcomp
        cudf::cudf spdlog::spdlog fmt::fmt Arrow::arrow_static Parquet::parquet_static)

target_include_directories(pandas_arrow
        PRIVATE ${Boost_INCLUDE_DIRS}
        PUBLIC src)

add_library(pd::pandas_arrow ALIAS pandas_arrow)

IF( BUILD_TEST )
    add_subdirectory(tests)
ENDIF()

add_executable(main main.cpp)
target_link_libraries(main PRIVATE pandas_arrow)