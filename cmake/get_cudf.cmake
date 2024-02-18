include_guard(GLOBAL)

set(CUDF_VERSION "24.02.00") # stable version
set(ARROW_VERSION "14.0.1") # WE NEED TO KEEP THIS VERSION AT 14.0.1 UNTIL WE HAVE arro/utils/span.h fix

rapids_cpm_find(
        cudf ${CUDF_VERSION}
        GLOBAL_TARGETS cudf
        CPM_ARGS
        GIT_REPOSITORY https://github.com/rapidsai/cudf.git
        GIT_SHALLOW TRUE SOURCE_SUBDIR cpp
        OPTIONS "CUDF_ENABLE_ARROW_PARQUET ON"
        "CUDF_USE_PER_THREAD_DEFAULT_STREAM ON"
        "CUDF_USE_ARROW_STATIC OFF"
        "BUILD_SHARED_LIBS ON"
        "CUDF_VERSION_Arrow ${ARROW_VERSION}"
        "BUILD_TESTS OFF"   # Disable building tests
        "BUILD_BENCHMARKS OFF" # Disable building benchmarks
        BUILD_EXPORT_SET rapid_cudf_test-exports)

include(${rapids-cmake-dir}/cpm/spdlog.cmake)
rapids_cpm_spdlog(BUILD_EXPORT_SET rapid_cudf_test-exports )

include(${rapids-cmake-dir}/cpm/fmt.cmake)
rapids_cpm_fmt(BUILD_EXPORT_SET rapid_cudf_test-exports)
