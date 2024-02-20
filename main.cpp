//
// Created by dewe on 2/19/24.
//
#include "cudf/table/table.hpp"
#include "cudf/io/parquet.hpp"
#include "filesystem"
#include "arrow/api.h"
#include "pandas_arrow.h"

int main() {
    using namespace cudf::io;

    auto AAPLFolder = "/home/dewe/SAMResearch/Database/Trades/Stocks/AAPL";

    std::vector<std::string> files;
    for (const auto &entry: std::filesystem::directory_iterator(AAPLFolder)) {
        files.push_back(entry.path().string());
    }

    auto option = parquet_reader_options::builder(source_info{files});
    auto df = read_parquet(option);

    return 0;
}