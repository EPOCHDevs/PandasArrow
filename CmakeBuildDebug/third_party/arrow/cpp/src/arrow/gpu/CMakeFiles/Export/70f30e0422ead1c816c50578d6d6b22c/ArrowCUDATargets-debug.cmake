#----------------------------------------------------------------
# Generated CMake target import file for configuration "DEBUG".
#----------------------------------------------------------------

# Commands may need to know the format version.
set(CMAKE_IMPORT_FILE_VERSION 1)

# Import target "ArrowCUDA::arrow_cuda_shared" for configuration "DEBUG"
set_property(TARGET ArrowCUDA::arrow_cuda_shared APPEND PROPERTY IMPORTED_CONFIGURATIONS DEBUG)
set_target_properties(ArrowCUDA::arrow_cuda_shared PROPERTIES
  IMPORTED_LOCATION_DEBUG "${_IMPORT_PREFIX}/lib/libarrow_cuda.so.1600.0.0"
  IMPORTED_SONAME_DEBUG "libarrow_cuda.so.1600"
  )

list(APPEND _cmake_import_check_targets ArrowCUDA::arrow_cuda_shared )
list(APPEND _cmake_import_check_files_for_ArrowCUDA::arrow_cuda_shared "${_IMPORT_PREFIX}/lib/libarrow_cuda.so.1600.0.0" )

# Commands beyond this point should not need to know the version.
set(CMAKE_IMPORT_FILE_VERSION)
