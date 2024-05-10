cmake_minimum_required(VERSION 3.27 FATAL_ERROR)

set(CMAKE_CUDA_COMPILER "/usr/local/cuda/bin/nvcc"  CACHE PATH "CUDA COMPILER")

option(BUILD_TEST OFF)
option(ENABLE_GPU OFF)

set(EPOCH_LAB "$ENV{HOME}/EpochLab"  CACHE PATH "SAM RESEARCH FOLDER")
include(${EPOCH_LAB}/shared_header.cmake)

if (ENABLE_GPU)
    rapids_cuda_init_architectures(PandasArrow)
    project(PandasArrow LANGUAGES C CXX CUDA)
else ()
    project(PandasArrow LANGUAGES C CXX)
endif ()

add_definitions(-DGLOG_USE_GLOG_EXPORT)

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

find_package(Boost REQUIRED COMPONENTS filesystem system date_time )
find_package(tabulate CONFIG REQUIRED)
find_package(TBB CONFIG REQUIRED)
find_package(Arrow CONFIG REQUIRED)
find_package(Parquet CONFIG REQUIRED)

add_library(pandas_arrow
        src/series.cpp
        src/scalar.cpp
        src/dataframe.cpp
        src/core.cpp
        src/resample.cpp
        src/concat.cpp)

if (ENABLE_GPU)
    target_sources(pandas_arrow PRIVATE
            src/cudf/cudf_utils.cpp
            src/cudf/dataframe.cpp
            src/cudf/scalar.cpp
            src/cudf/series.cpp)

    target_link_libraries(pandas_arrow
            PRIVATE spdlog::spdlog fmt::fmt nvcomp::nvcomp cudf::cudf)
endif()

target_link_libraries(pandas_arrow PRIVATE TBB::tbb parquet_static  PUBLIC arrow_static)

target_include_directories(pandas_arrow
        PRIVATE ${Boost_INCLUDE_DIRS} ${AWSSDK_INCLUDE_DIRS}
        PUBLIC src)

add_library(pd::pandas_arrow ALIAS pandas_arrow)

IF( BUILD_TEST )
    add_subdirectory(tests)
ENDIF()

add_executable(main main.cpp)
target_link_libraries(main pandas_arrow)