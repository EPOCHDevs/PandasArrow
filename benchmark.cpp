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

std::string GetStringFromDF(pd::DataFrame const& df, bool toRecords) {
    rapidjson::Document doc;
    auto jsonData = df.toJSON(doc.GetAllocator(), {}, "t", toRecords);
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    jsonData->Accept(writer);
    return buffer.GetString();
}

std::pair<std::shared_ptr<arrow::Buffer>, std::basic_string_view<uint8_t>> GetBlobFromDF(pd::DataFrame const& df) {
    auto buffer = pd::ReturnOrThrowOnFailure(df.toBinary({}, "t"));
    buffer->address();
    return {buffer, {buffer->data(), static_cast<size_t>(buffer->size()) } };
}



int main() {
    auto xlarge = pd::DataFrame::readParquet("/home/adesola/EpochLab/Database/S3/DailyBars/Options/SPY.parquet.gzip");
    auto large = pd::DataFrame::readParquet("s3://epoch-db/MinuteBars/Stocks/AAPL.parquet.gzip");
    auto mid = large.slice(0, 100000);
    auto small = large.slice(0, 10000);
    auto xsmall = large.slice(0, 2);

    int n_runs = 10;

    std::once_flag f1, f2, f3;
    for (auto && df: {xsmall, small, mid, large, xlarge})
    {
        int64_t jsonRowDurations{}, jsonColumnDurations{}, binaryDurations{};
        std::chrono::TimeIt timer;

        std::string rowJSONData, columnJSONData;
        std::pair<std::shared_ptr<arrow::Buffer>, std::basic_string_view<uint8_t>> blob;
        for (int run = 0; run < n_runs; run++) {

            timer.reset();
            rowJSONData = GetStringFromDF(df, true);
            jsonRowDurations += timer.duration();

            timer.reset();
            columnJSONData = GetStringFromDF(df, false);
            jsonColumnDurations += timer.duration();

            timer.reset();
            blob = GetBlobFromDF(df);
            binaryDurations += timer.duration();
        }

        SPDLOG_INFO("{} rows took json-rows {} ms to serialize.", df.num_rows(), (double)jsonRowDurations/10.0);
        SPDLOG_INFO("{} rows took json-columns {} ms to serialize.", df.num_rows(), (double)jsonColumnDurations/10.0);
        SPDLOG_INFO("{} rows took binary {} ms to serialize.", df.num_rows(), (double)binaryDurations/10.0);

        jsonRowDurations = {};
        jsonColumnDurations = {};
        binaryDurations = {};

        std::optional<pd::DataFrame> jsonRowDF, jsonColumnDF, binaryDF;
        for (int run = 0; run < n_runs; run++) {

            timer.reset();
            jsonRowDF = pd::DataFrame::readJSON(rowJSONData, df.array()->schema());
            jsonRowDurations += timer.duration();

            timer.reset();
            jsonColumnDF = pd::DataFrame::readJSON(columnJSONData, df.array()->schema());
            jsonColumnDurations += timer.duration();

            timer.reset();
            std::basic_string_view<char> imm{reinterpret_cast<const char*>(blob.second.data() ), blob.second.size()}; // test no loss
            binaryDF = pd::DataFrame::readBinary(std::basic_string_view<uint8_t>{reinterpret_cast<const uint8_t*>(imm.data()), imm.size()});
            binaryDurations += timer.duration();
        }

        SPDLOG_INFO("{} rows took row-json {} ms to deserialize.: .",  df.num_rows(), (double)jsonRowDurations/10.0);
        std::call_once(f1, [&](){
            std::cout << *jsonRowDF << std::endl;
        });

        SPDLOG_INFO("{} rows took column-json {} ms to deserialize.: .",  df.num_rows(), (double)jsonColumnDurations/10.0);
        std::call_once(f2, [&](){
            std::cout << *jsonColumnDF << std::endl;
        });

        SPDLOG_INFO("{} rows took binary {} ms to deserialize.: .", df.num_rows(), (double)binaryDurations/10.0);
        std::call_once(f3, [&](){
            std::cout << *binaryDF << std::endl;
        });
    }

    return 0;
}
