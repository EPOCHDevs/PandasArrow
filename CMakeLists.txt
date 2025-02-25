cmake_minimum_required(VERSION 3.27 FATAL_ERROR)

set(CMAKE_CUDA_COMPILER "/usr/local/cuda/bin/nvcc"  CACHE PATH "CUDA COMPILER")

option(BUILD_TEST OFF)
option(ENABLE_GPU OFF)

set(EPOCH_LAB "$ENV{HOME}/EpochLab"  CACHE PATH "Main Epoch Lab Folder")
include(${EPOCH_LAB}/shared_header.cmake)

if (ENABLE_GPU)
    rapids_cuda_init_architectures(PandasArrow)
    project(PandasArrow LANGUAGES C CXX CUDA)
else ()
    project(PandasArrow LANGUAGES C CXX)
endif ()

set(Boost_NO_WARN_NEW_VERSIONS ON)
option(Boost_USE_STATIC_LIBS  ON)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

rapids_cpm_init()

if (ENABLE_GPU)
    # add third party dependencies
    rapids_cuda_init_runtime(USE_STATIC ON)
    include(${EPOCH_CMAKE}/get_cudf.cmake)
endif ()

include(${EPOCH_CMAKE}/get_cpp_dataframe.cmake)
find_package(Boost REQUIRED COMPONENTS filesystem system date_time )
find_package(tabulate CONFIG REQUIRED)
find_package(TBB CONFIG REQUIRED)
find_package(Arrow CONFIG REQUIRED)
find_package(Parquet CONFIG REQUIRED)

add_library(pandas_arrow
        src/aws_s3_reader.cpp
        src/ndframe.cpp
        src/series.cpp
        src/scalar.cpp
        src/dataframe.cpp
        src/core.cpp
        src/resample.cpp
        src/concat.cpp
#        src/json_utils.cpp
        src/list_s3_files.cpp)

if (ENABLE_GPU)
    target_sources(pandas_arrow PRIVATE
            src/cudf/cudf_utils.cpp
            src/cudf/dataframe.cpp
            src/cudf/scalar.cpp
            src/cudf/series.cpp)

    target_link_libraries(pandas_arrow
            PRIVATE nvcomp::nvcomp cudf::cudf)
endif()


find_package(spdlog CONFIG REQUIRED)
find_package(fmt CONFIG REQUIRED)

target_link_libraries(pandas_arrow
        PRIVATE TBB::tbb parquet_static spdlog::spdlog_header_only fmt::fmt-header-only
        PUBLIC arrow_static curl ssl crypto DataFrame)

target_compile_options(pandas_arrow PRIVATE -Wall -Wextra -Werror
        # https://linear.app/epoch-inc/issue/STR-45/remove-wno-strict-aliasing-check
        PUBLIC -Wno-strict-aliasing)

# https://linear.app/epoch-inc/issue/STR-44/remove-rapidjson-dependence-in-pandas-arrow
#target_compile_definitions(pandas_arrow PUBLIC -DRAPIDJSON_HAS_STDSTRING=1)

target_include_directories(pandas_arrow
        PRIVATE ${Boost_INCLUDE_DIRS} ${EPOCH_LAB_COMMON_HEADERS}
        PUBLIC src)

add_library(pd::pandas_arrow ALIAS pandas_arrow)

IF( BUILD_TEST )
    add_subdirectory(tests)
ENDIF()
