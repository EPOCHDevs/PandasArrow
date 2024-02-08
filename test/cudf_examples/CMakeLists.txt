
add_executable(load_and_create_df load_and_create_df.cpp )
target_include_directories(load_and_create_df PRIVATE ../../..)
target_link_libraries(load_and_create_df PRIVATE pandas_arrow)