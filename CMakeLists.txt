cmake_minimum_required(VERSION 3.27 FATAL_ERROR)
set(CMAKE_TOOLCHAIN_FILE $ENV{HOME}/vcpkg/scripts/buildsystems/vcpkg.cmake CACHE STRING "Vcpkg toolchain file")
set(CMAKE_CUDA_COMPILER "/usr/local/cuda/bin/nvcc"  CACHE PATH "CUDA COMPILER")

project(PandasArrow LANGUAGES C CXX CUDA)

option(BUILD_TEST OFF)
set(Boost_NO_WARN_NEW_VERSIONS ON)
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CUDA_ARCHITECTURES native)
option(Boost_USE_STATIC_LIBS  ON)

ADD_SUBDIRECTORY(third_party)

find_package(Boost REQUIRED COMPONENTS filesystem system date_time )
find_package(tabulate CONFIG REQUIRED)
find_package(TBB CONFIG REQUIRED)
find_package(spdlog CONFIG REQUIRED)
find_package(fmt CONFIG REQUIRED)

add_library(pandas_arrow series.cpp scalar.cpp
        dataframe.cpp core.cpp resample.cpp concat.cpp)
ADD_SUBDIRECTORY(cudf)

target_link_libraries(pandas_arrow PUBLIC
        ${Boost_LIBRARIES}
        TBB::tbb tabulate::tabulate
        arrow_shared parquet_shared arrow_cuda_shared cudf::cudf spdlog::spdlog fmt::fmt)

set(ARROW_INCLUDES "third_party/installed/arrow/include")
set(CUDF_INCLUDES "third_party/installed/cudf/include")
set(RAPIDS_INCLUDES ${CUDF_INCLUDES}/libcudf/rapids)
set(LIB_CUDA_CXX_INCLUDES ${RAPIDS_INCLUDES}/libcudacxx)
set(THRUST_INCLUDES ${RAPIDS_INCLUDES}/thrust)
set(CUB_INCLUDES ${RAPIDS_INCLUDES}/cub)

target_include_directories(pandas_arrow PUBLIC .
        ${Boost_INCLUDE_DIRS} ${ARROW_INCLUDES} ${CUDF_INCLUDES}
        ${LIB_CUDA_CXX_INCLUDES})

file(MAKE_DIRECTORY ${ARROW_INCLUDES})
target_include_directories(cudf PRIVATE ${ARROW_INCLUDES})
target_compile_options(pandas_arrow PRIVATE -Werror)
target_compile_definitions(pandas_arrow PUBLIC -DLIBCUDACXX_ENABLE_EXPERIMENTAL_MEMORY_RESOURCE)

file(GLOB MY_FOLDER_CONTENTS "${ARROW_INCLUDES}/*")
if(MY_FOLDER_CONTENTS STREQUAL "")
    # Define a custom target for installing Arrow (if necessary)
    add_custom_target(install_arrow
            COMMAND "${CMAKE_COMMAND}" --build . --target install
            WORKING_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/third_party/arrow/cpp"
    )
    add_dependencies(pandas_arrow install_arrow)
endif()

file(GLOB MY_FOLDER_CONTENTS "${CUDF_INCLUDES}/*")
if(MY_FOLDER_CONTENTS STREQUAL "")
    # Define a custom target for installing Arrow (if necessary)
    add_custom_target(install_cudf
            COMMAND "${CMAKE_COMMAND}" --build . --target install
            WORKING_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/third_party/cudf/cpp"
    )

    add_dependencies(pandas_arrow install_cudf)
endif()

IF( BUILD_TEST)
    add_subdirectory(test)
ENDIF()