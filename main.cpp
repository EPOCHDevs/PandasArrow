//
// Created by adesola on 5/10/24.
//
#include "pandas_arrow.h"
#include <rapidjson/document.h>
#include <rapidjson/writer.h>
#include <rapidjson/stringbuffer.h>
#include <spdlog/spdlog.h>


int main() {
    auto file = pd::DataFrame::readParquet("/home/adesola/EpochLab/Database/S3/MinuteBarstatus/Stocks/AAPL.parquet.gzip");

    auto start = std::chrono::high_resolution_clock::now();

    rapidjson::Document doc;
    doc.SetObject();

    auto jsonDataResult = file.toJSON(doc.GetAllocator());

    auto end = std::chrono::high_resolution_clock::now();

    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    std::cout << "Conversion to JSON took " << duration << " milliseconds to serialize to JSON length " << file.num_rows() << std::endl;

     start = std::chrono::high_resolution_clock::now();
    auto binary = file.toBinary();
     end = std::chrono::high_resolution_clock::now();

    duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    std::cout << "Conversion to Buffer took " << duration << " milliseconds to serialize to Binary length " << file.num_rows() << std::endl;

    if (jsonDataResult.ok()) {
        doc.AddMember("data", jsonDataResult.ValueOrDie(), doc.GetAllocator());

        // Convert the RapidJSON Document to a string
        rapidjson::StringBuffer buffer;
        rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
        doc.Accept(writer);

        // Print the JSON string
        SPDLOG_TRACE(buffer.GetString());
    } else {
        std::cerr << "Error converting to JSON: " << jsonDataResult.status().ToString() << std::endl;
    }

    return 0;
}
