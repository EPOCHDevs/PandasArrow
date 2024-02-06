# Install script for directory: /home/dewe/SAMResearch/StratifyX/cpp/third_party/pandas_arrow/third_party/arrow/cpp/src/arrow/compute

# Set the install prefix
if(NOT DEFINED CMAKE_INSTALL_PREFIX)
  set(CMAKE_INSTALL_PREFIX "/home/dewe/SAMResearch/StratifyX/cpp/third_party/pandas_arrow/third_party/arrow/../installed/arrow")
endif()
string(REGEX REPLACE "/$" "" CMAKE_INSTALL_PREFIX "${CMAKE_INSTALL_PREFIX}")

# Set the install configuration name.
if(NOT DEFINED CMAKE_INSTALL_CONFIG_NAME)
  if(BUILD_TYPE)
    string(REGEX REPLACE "^[^A-Za-z0-9_]+" ""
           CMAKE_INSTALL_CONFIG_NAME "${BUILD_TYPE}")
  else()
    set(CMAKE_INSTALL_CONFIG_NAME "DEBUG")
  endif()
  message(STATUS "Install configuration: \"${CMAKE_INSTALL_CONFIG_NAME}\"")
endif()

# Set the component getting installed.
if(NOT CMAKE_INSTALL_COMPONENT)
  if(COMPONENT)
    message(STATUS "Install component: \"${COMPONENT}\"")
    set(CMAKE_INSTALL_COMPONENT "${COMPONENT}")
  else()
    set(CMAKE_INSTALL_COMPONENT)
  endif()
endif()

# Install shared libraries without execute permission?
if(NOT DEFINED CMAKE_INSTALL_SO_NO_EXE)
  set(CMAKE_INSTALL_SO_NO_EXE "1")
endif()

# Is this installation the result of a crosscompile?
if(NOT DEFINED CMAKE_CROSSCOMPILING)
  set(CMAKE_CROSSCOMPILING "FALSE")
endif()

# Set default install directory permissions.
if(NOT DEFINED CMAKE_OBJDUMP)
  set(CMAKE_OBJDUMP "/usr/bin/objdump")
endif()

if(CMAKE_INSTALL_COMPONENT STREQUAL "Unspecified" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/include/arrow/compute" TYPE FILE FILES
    "/home/dewe/SAMResearch/StratifyX/cpp/third_party/pandas_arrow/third_party/arrow/cpp/src/arrow/compute/api.h"
    "/home/dewe/SAMResearch/StratifyX/cpp/third_party/pandas_arrow/third_party/arrow/cpp/src/arrow/compute/api_additional.h"
    "/home/dewe/SAMResearch/StratifyX/cpp/third_party/pandas_arrow/third_party/arrow/cpp/src/arrow/compute/api_aggregate.h"
    "/home/dewe/SAMResearch/StratifyX/cpp/third_party/pandas_arrow/third_party/arrow/cpp/src/arrow/compute/api_scalar.h"
    "/home/dewe/SAMResearch/StratifyX/cpp/third_party/pandas_arrow/third_party/arrow/cpp/src/arrow/compute/api_vector.h"
    "/home/dewe/SAMResearch/StratifyX/cpp/third_party/pandas_arrow/third_party/arrow/cpp/src/arrow/compute/cast.h"
    "/home/dewe/SAMResearch/StratifyX/cpp/third_party/pandas_arrow/third_party/arrow/cpp/src/arrow/compute/exec.h"
    "/home/dewe/SAMResearch/StratifyX/cpp/third_party/pandas_arrow/third_party/arrow/cpp/src/arrow/compute/expression.h"
    "/home/dewe/SAMResearch/StratifyX/cpp/third_party/pandas_arrow/third_party/arrow/cpp/src/arrow/compute/function.h"
    "/home/dewe/SAMResearch/StratifyX/cpp/third_party/pandas_arrow/third_party/arrow/cpp/src/arrow/compute/function_options.h"
    "/home/dewe/SAMResearch/StratifyX/cpp/third_party/pandas_arrow/third_party/arrow/cpp/src/arrow/compute/kernel.h"
    "/home/dewe/SAMResearch/StratifyX/cpp/third_party/pandas_arrow/third_party/arrow/cpp/src/arrow/compute/key_hash.h"
    "/home/dewe/SAMResearch/StratifyX/cpp/third_party/pandas_arrow/third_party/arrow/cpp/src/arrow/compute/key_map.h"
    "/home/dewe/SAMResearch/StratifyX/cpp/third_party/pandas_arrow/third_party/arrow/cpp/src/arrow/compute/light_array.h"
    "/home/dewe/SAMResearch/StratifyX/cpp/third_party/pandas_arrow/third_party/arrow/cpp/src/arrow/compute/ordering.h"
    "/home/dewe/SAMResearch/StratifyX/cpp/third_party/pandas_arrow/third_party/arrow/cpp/src/arrow/compute/registry.h"
    "/home/dewe/SAMResearch/StratifyX/cpp/third_party/pandas_arrow/third_party/arrow/cpp/src/arrow/compute/type_fwd.h"
    "/home/dewe/SAMResearch/StratifyX/cpp/third_party/pandas_arrow/third_party/arrow/cpp/src/arrow/compute/util.h"
    )
endif()

if(CMAKE_INSTALL_COMPONENT STREQUAL "Unspecified" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/pkgconfig" TYPE FILE FILES "/home/dewe/SAMResearch/StratifyX/cpp/third_party/pandas_arrow/CmakeBuildDebug/third_party/arrow/cpp/src/arrow/compute/DEBUG/arrow-compute.pc")
endif()

if(NOT CMAKE_INSTALL_LOCAL_ONLY)
  # Include the install script for the subdirectory.
  include("/home/dewe/SAMResearch/StratifyX/cpp/third_party/pandas_arrow/CmakeBuildDebug/third_party/arrow/cpp/src/arrow/compute/kernels/cmake_install.cmake")
endif()

if(NOT CMAKE_INSTALL_LOCAL_ONLY)
  # Include the install script for the subdirectory.
  include("/home/dewe/SAMResearch/StratifyX/cpp/third_party/pandas_arrow/CmakeBuildDebug/third_party/arrow/cpp/src/arrow/compute/row/cmake_install.cmake")
endif()

