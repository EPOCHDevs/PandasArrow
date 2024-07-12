//
// Created by adesola on 5/10/24.
//
#include "pandas_arrow.h"
#include <fstream>
#include <filesystem>

#undef PD_MAX_ROW_TO_PRINT
#define PD_MAX_ROW_TO_PRINT 10

int main() {
    try {
        // Read the Parquet file and slice the first 10 rows
        auto data = pd::DataFrame::readParquet("/home/adesola/EpochLab/Database/S3/MinuteBars/Stocks/AAPL.parquet.gzip").slice(0, 10);

        // Convert the DataFrame to a binary buffer in Arrow format
        auto buffer = pd::ReturnOrThrowOnFailure(data.toBinary({}, "t"));

        // Create file paths
        std::filesystem::path currentPath = std::filesystem::current_path();
        std::filesystem::path streamFilePath = currentPath / "stream_file.arrow";
        std::filesystem::path expectedFilePath = currentPath / "data.txt";

        // Open file streams
        std::ofstream fileStream(streamFilePath);
        std::ofstream expectedStream(expectedFilePath);

        // Check if files are open
        if (!fileStream.is_open()) {
            throw std::runtime_error("Unable to open file stream_file.arrow for writing.");
        }
        if (!expectedStream.is_open()) {
            throw std::runtime_error("Unable to open file data.txt for writing.");
        }

        // Write the buffer to the Arrow file
        fileStream << buffer->ToString();
        fileStream.close();

        // Write the DataFrame to the text file
        expectedStream << data;
        expectedStream.close();

        std::cout << "Files have been written successfully." << std::endl;
    } catch (const std::exception& ex) {
        std::cerr << "An error occurred: " << ex.what() << std::endl;
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}
