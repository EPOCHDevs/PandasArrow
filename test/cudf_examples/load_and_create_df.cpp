//
// Created by dewe on 2/6/24.
//
#include "cudf/dataframe.h"
#include <vector>
#include "pandas_arrow.h"
#include "time_it.h"


int main()
{
    auto dailyData = pd::DataFrame::readParquet("/home/dewe/SAMResearch/Database/DailyBars/Crypto/BTC-USD.parquet.gzip");
    auto minuteData = pd::DataFrame::readParquet("/home/dewe/SAMResearch/Database/MinuteBars/Crypto/BTC-USD.parquet.gzip");

    auto directories = std::filesystem::directory_iterator("/home/dewe/SAMResearch/Database/Trades/Stocks/AAPL");
    std::vector<pd::DataFrame> dataFrames;
    for(auto const& path : directories | std::views::take(500))
    {
        dataFrames.push_back(pd::DataFrame::readParquet(path));
    }
    auto tickData = pd::concat(dataFrames);
    std::cout << "Tick Data Size: " << tickData.num_rows() << "\n";

    pd::GPUDataframe const tickGPUData{tickData};
    {
        TimeIt<std::nano> ("GPU Addition of Tick");
        auto addGPU = (tickGPUData + tickGPUData) * tickGPUData;
    }

    {
        TimeIt<std::nano> ("CPU Addition of Tick");
        auto addCPU = (tickData + tickData) * tickData;
    }

    std::cout << "Minute Data Size: " << minuteData.num_rows() << "\n";

    pd::GPUDataframe const minuteGPUData{minuteData};
    {
        TimeIt<std::nano> ("GPU Addition of MINUTE");
        auto addGPU = (minuteGPUData + minuteGPUData) * minuteGPUData;
    }

    {
        TimeIt<std::nano> ("CPU Addition of MINUTE");
        auto addCPU = (minuteData + minuteData) * minuteData;
    }

    std::cout << "Daily Data Size: " << dailyData.num_rows() << "\n";
    pd::GPUDataframe const dailyGPUData{dailyData};
    {
        TimeIt<std::nano> ("GPU Addition of daily");
        auto addGPU = (dailyGPUData + dailyGPUData) * dailyGPUData;
    }

    {
        TimeIt<std::nano> ("CPU Addition of daily");
        auto addCPU = (dailyData + dailyData) * dailyData;
    }

    return 0;
}