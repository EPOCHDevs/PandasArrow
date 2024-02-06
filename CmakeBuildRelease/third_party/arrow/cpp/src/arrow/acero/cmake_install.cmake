# Install script for directory: /home/dewe/SAMResearch/StratifyX/cpp/third_party/pandas_arrow/third_party/arrow/cpp/src/arrow/acero

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
    set(CMAKE_INSTALL_CONFIG_NAME "RELEASE")
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
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/include/arrow/acero" TYPE FILE FILES
    "/home/dewe/SAMResearch/StratifyX/cpp/third_party/pandas_arrow/third_party/arrow/cpp/src/arrow/acero/accumulation_queue.h"
    "/home/dewe/SAMResearch/StratifyX/cpp/third_party/pandas_arrow/third_party/arrow/cpp/src/arrow/acero/aggregate_node.h"
    "/home/dewe/SAMResearch/StratifyX/cpp/third_party/pandas_arrow/third_party/arrow/cpp/src/arrow/acero/api.h"
    "/home/dewe/SAMResearch/StratifyX/cpp/third_party/pandas_arrow/third_party/arrow/cpp/src/arrow/acero/asof_join_node.h"
    "/home/dewe/SAMResearch/StratifyX/cpp/third_party/pandas_arrow/third_party/arrow/cpp/src/arrow/acero/backpressure_handler.h"
    "/home/dewe/SAMResearch/StratifyX/cpp/third_party/pandas_arrow/third_party/arrow/cpp/src/arrow/acero/benchmark_util.h"
    "/home/dewe/SAMResearch/StratifyX/cpp/third_party/pandas_arrow/third_party/arrow/cpp/src/arrow/acero/bloom_filter.h"
    "/home/dewe/SAMResearch/StratifyX/cpp/third_party/pandas_arrow/third_party/arrow/cpp/src/arrow/acero/exec_plan.h"
    "/home/dewe/SAMResearch/StratifyX/cpp/third_party/pandas_arrow/third_party/arrow/cpp/src/arrow/acero/hash_join.h"
    "/home/dewe/SAMResearch/StratifyX/cpp/third_party/pandas_arrow/third_party/arrow/cpp/src/arrow/acero/hash_join_dict.h"
    "/home/dewe/SAMResearch/StratifyX/cpp/third_party/pandas_arrow/third_party/arrow/cpp/src/arrow/acero/hash_join_node.h"
    "/home/dewe/SAMResearch/StratifyX/cpp/third_party/pandas_arrow/third_party/arrow/cpp/src/arrow/acero/map_node.h"
    "/home/dewe/SAMResearch/StratifyX/cpp/third_party/pandas_arrow/third_party/arrow/cpp/src/arrow/acero/options.h"
    "/home/dewe/SAMResearch/StratifyX/cpp/third_party/pandas_arrow/third_party/arrow/cpp/src/arrow/acero/order_by_impl.h"
    "/home/dewe/SAMResearch/StratifyX/cpp/third_party/pandas_arrow/third_party/arrow/cpp/src/arrow/acero/partition_util.h"
    "/home/dewe/SAMResearch/StratifyX/cpp/third_party/pandas_arrow/third_party/arrow/cpp/src/arrow/acero/pch.h"
    "/home/dewe/SAMResearch/StratifyX/cpp/third_party/pandas_arrow/third_party/arrow/cpp/src/arrow/acero/query_context.h"
    "/home/dewe/SAMResearch/StratifyX/cpp/third_party/pandas_arrow/third_party/arrow/cpp/src/arrow/acero/schema_util.h"
    "/home/dewe/SAMResearch/StratifyX/cpp/third_party/pandas_arrow/third_party/arrow/cpp/src/arrow/acero/task_util.h"
    "/home/dewe/SAMResearch/StratifyX/cpp/third_party/pandas_arrow/third_party/arrow/cpp/src/arrow/acero/test_nodes.h"
    "/home/dewe/SAMResearch/StratifyX/cpp/third_party/pandas_arrow/third_party/arrow/cpp/src/arrow/acero/time_series_util.h"
    "/home/dewe/SAMResearch/StratifyX/cpp/third_party/pandas_arrow/third_party/arrow/cpp/src/arrow/acero/tpch_node.h"
    "/home/dewe/SAMResearch/StratifyX/cpp/third_party/pandas_arrow/third_party/arrow/cpp/src/arrow/acero/type_fwd.h"
    "/home/dewe/SAMResearch/StratifyX/cpp/third_party/pandas_arrow/third_party/arrow/cpp/src/arrow/acero/unmaterialized_table.h"
    "/home/dewe/SAMResearch/StratifyX/cpp/third_party/pandas_arrow/third_party/arrow/cpp/src/arrow/acero/util.h"
    "/home/dewe/SAMResearch/StratifyX/cpp/third_party/pandas_arrow/third_party/arrow/cpp/src/arrow/acero/visibility.h"
    )
endif()

if(CMAKE_INSTALL_COMPONENT STREQUAL "Unspecified" OR NOT CMAKE_INSTALL_COMPONENT)
  foreach(file
      "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/libarrow_acero.so.1600.0.0"
      "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/libarrow_acero.so.1600"
      )
    if(EXISTS "${file}" AND
       NOT IS_SYMLINK "${file}")
      file(RPATH_CHECK
           FILE "${file}"
           RPATH "")
    endif()
  endforeach()
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib" TYPE SHARED_LIBRARY FILES
    "/home/dewe/SAMResearch/StratifyX/cpp/third_party/pandas_arrow/CmakeBuildRelease/third_party/arrow/cpp/release/libarrow_acero.so.1600.0.0"
    "/home/dewe/SAMResearch/StratifyX/cpp/third_party/pandas_arrow/CmakeBuildRelease/third_party/arrow/cpp/release/libarrow_acero.so.1600"
    )
  foreach(file
      "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/libarrow_acero.so.1600.0.0"
      "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/libarrow_acero.so.1600"
      )
    if(EXISTS "${file}" AND
       NOT IS_SYMLINK "${file}")
      file(RPATH_CHANGE
           FILE "${file}"
           OLD_RPATH "/home/dewe/SAMResearch/StratifyX/cpp/third_party/pandas_arrow/CmakeBuildRelease/third_party/arrow/cpp/release:"
           NEW_RPATH "")
      if(CMAKE_INSTALL_DO_STRIP)
        execute_process(COMMAND "/usr/bin/strip" "${file}")
      endif()
    endif()
  endforeach()
endif()

if(CMAKE_INSTALL_COMPONENT STREQUAL "Unspecified" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib" TYPE SHARED_LIBRARY FILES "/home/dewe/SAMResearch/StratifyX/cpp/third_party/pandas_arrow/CmakeBuildRelease/third_party/arrow/cpp/release/libarrow_acero.so")
endif()

if(CMAKE_INSTALL_COMPONENT STREQUAL "Unspecified" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/cmake/ArrowAcero" TYPE FILE FILES
    "/home/dewe/SAMResearch/StratifyX/cpp/third_party/pandas_arrow/CmakeBuildRelease/third_party/arrow/cpp/src/arrow/acero/ArrowAceroConfig.cmake"
    "/home/dewe/SAMResearch/StratifyX/cpp/third_party/pandas_arrow/CmakeBuildRelease/third_party/arrow/cpp/src/arrow/acero/ArrowAceroConfigVersion.cmake"
    )
endif()

if(CMAKE_INSTALL_COMPONENT STREQUAL "Unspecified" OR NOT CMAKE_INSTALL_COMPONENT)
  if(EXISTS "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/cmake/ArrowAcero/ArrowAceroTargets.cmake")
    file(DIFFERENT _cmake_export_file_changed FILES
         "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/cmake/ArrowAcero/ArrowAceroTargets.cmake"
         "/home/dewe/SAMResearch/StratifyX/cpp/third_party/pandas_arrow/CmakeBuildRelease/third_party/arrow/cpp/src/arrow/acero/CMakeFiles/Export/d7d2c1ddcde43433b8c5420fc45422e9/ArrowAceroTargets.cmake")
    if(_cmake_export_file_changed)
      file(GLOB _cmake_old_config_files "$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/cmake/ArrowAcero/ArrowAceroTargets-*.cmake")
      if(_cmake_old_config_files)
        string(REPLACE ";" ", " _cmake_old_config_files_text "${_cmake_old_config_files}")
        message(STATUS "Old export file \"$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/lib/cmake/ArrowAcero/ArrowAceroTargets.cmake\" will be replaced.  Removing files [${_cmake_old_config_files_text}].")
        unset(_cmake_old_config_files_text)
        file(REMOVE ${_cmake_old_config_files})
      endif()
      unset(_cmake_old_config_files)
    endif()
    unset(_cmake_export_file_changed)
  endif()
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/cmake/ArrowAcero" TYPE FILE FILES "/home/dewe/SAMResearch/StratifyX/cpp/third_party/pandas_arrow/CmakeBuildRelease/third_party/arrow/cpp/src/arrow/acero/CMakeFiles/Export/d7d2c1ddcde43433b8c5420fc45422e9/ArrowAceroTargets.cmake")
  if(CMAKE_INSTALL_CONFIG_NAME MATCHES "^([Rr][Ee][Ll][Ee][Aa][Ss][Ee])$")
    file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/cmake/ArrowAcero" TYPE FILE FILES "/home/dewe/SAMResearch/StratifyX/cpp/third_party/pandas_arrow/CmakeBuildRelease/third_party/arrow/cpp/src/arrow/acero/CMakeFiles/Export/d7d2c1ddcde43433b8c5420fc45422e9/ArrowAceroTargets-release.cmake")
  endif()
endif()

if(CMAKE_INSTALL_COMPONENT STREQUAL "Unspecified" OR NOT CMAKE_INSTALL_COMPONENT)
  file(INSTALL DESTINATION "${CMAKE_INSTALL_PREFIX}/lib/pkgconfig" TYPE FILE FILES "/home/dewe/SAMResearch/StratifyX/cpp/third_party/pandas_arrow/CmakeBuildRelease/third_party/arrow/cpp/src/arrow/acero/RELEASE/arrow-acero.pc")
endif()

