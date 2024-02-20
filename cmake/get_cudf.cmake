include_guard(GLOBAL)

SET(CMAKE_PREFIX_PATH "${SAM_RESEARCH}/cache/cudf/lib/cmake")
find_package(cudf REQUIRED)
find_package(Arrow REQUIRED)
find_package(Parquet REQUIRED)
find_package(fmt REQUIRED)
find_package(spdlog REQUIRED)
find_package(nvcomp REQUIRED)

