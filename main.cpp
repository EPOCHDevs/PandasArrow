//
// Created by adesola on 5/10/24.
//
#include "pandas_arrow.h"

int main() {
    auto file = pd::DataFrame::readParquet("s3://epoch-database/DailyBars/Crypto/1INCH-USD.parquet.gzip");
    std::cout << file << "\n";
    return 0;
}