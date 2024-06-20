//
// Created by adesola on 5/10/24.
//
#include "pandas_arrow.h"
#include <rapidjson/document.h>
#include <rapidjson/writer.h>
#include <rapidjson/stringbuffer.h>
#include <spdlog/spdlog.h>

#undef PD_MAX_ROW_TO_PRINT
#define PD_MAX_ROW_TO_PRINT 10

namespace std::chrono {
    struct TimeIt {
        void reset() {
            start = high_resolution_clock::now();
        }
        auto duration() {
            auto end = high_resolution_clock::now();
            return duration_cast<milliseconds>(end - start).count();
        }
        std::chrono::time_point<system_clock, nanoseconds> start{high_resolution_clock::now()};
    };
}

std::string GetStringFromDF(pd::DataFrame const& df) {
    rapidjson::Document doc;
    auto jsonData = df.toJSON(doc.GetAllocator());
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    jsonData->Accept(writer);
    return buffer.GetString();
}

std::pair<std::shared_ptr<arrow::Buffer>, std::basic_string_view<uint8_t>> GetBlobFromDF(pd::DataFrame const& df) {
    auto buffer = pd::ReturnOrThrowOnFailure(df.toBinary());
    buffer->address();
    return {buffer, {buffer->data(), static_cast<size_t>(buffer->size()) } };
}

int main() {
    auto large = pd::DataFrame::readParquet("/home/adesola/EpochLab/Database/S3/MinuteBars/Stocks/AAPL.parquet.gzip");
    auto mid = large.slice(0, 100000);
    auto small = large.slice(0, 10000);
    auto xsmall = large.slice(0, 2);

    int n_runs = 10;

    std::once_flag f1, f2;
    for (auto && df: {xsmall, small, mid, large})
    {
        int64_t jsonDurations{}, binaryDurations{};
        std::chrono::TimeIt timer;

        std::string data;
        std::pair<std::shared_ptr<arrow::Buffer>, std::basic_string_view<uint8_t>> blob;
        for (int run = 0; run < n_runs; run++) {

            timer.reset();
            data = GetStringFromDF(df);
            jsonDurations += timer.duration();

            timer.reset();
            blob = GetBlobFromDF(df);
            binaryDurations += timer.duration();
        }

        SPDLOG_INFO("{} rows took json {} ms to serialize.", df.num_rows(), (double)jsonDurations/10.0);
        SPDLOG_INFO("{} rows took binary {} ms to serialize.", df.num_rows(), (double)binaryDurations/10.0);

        jsonDurations = {};
        binaryDurations = {};

        std::optional<pd::DataFrame> jsonDF, binaryDF;
        for (int run = 0; run < n_runs; run++) {

            timer.reset();
            jsonDF = pd::DataFrame::readJSON(data, df.array()->schema());
            jsonDurations += timer.duration();

            timer.reset();
            binaryDF = pd::DataFrame::readBinary(blob.second);
            binaryDurations += timer.duration();
        }

        SPDLOG_INFO("{} rows took json {} ms to deserialize.: .",  df.num_rows(), (double)jsonDurations/10.0);

        std::call_once(f1, [&](){
            std::cout << *jsonDF << std::endl;
        });

        SPDLOG_INFO("{} rows took binary {} ms to deserialize.: .", df.num_rows(), (double)binaryDurations/10.0);
        std::call_once(f2, [&](){
            std::cout << *binaryDF << std::endl;
        });
    }

    return 0;
}
