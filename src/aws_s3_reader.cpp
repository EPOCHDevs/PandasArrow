//
// Created by adesola on 5/10/24.
//
#include "aws_s3_reader.h"
#include <iostream>
#include <filesystem>
#include "fmt/format.h"
#include "spdlog/spdlog.h"


namespace arrow{
    AWSS3Reader::AWSS3Reader() {
        arrow::fs::S3GlobalOptions s3_options;

        s3_options.log_level = fs::S3LogLevel::Error;
        const auto status = fs::InitializeS3(s3_options);
        if (!status.ok()) {
            throw std::runtime_error(fmt::format("Failed to initialize S3 access: {}.", status.ToString()));
        }

        fs::S3Options options = fs::S3Options::Defaults();
        const auto aws_access_key = getenv("AWS_ACCESS_KEY_ID");
        const auto aws_secret_key = getenv("AWS_SECRET_ACCESS_KEY");
        const auto aws_region = getenv("AWS_REGION");

        if (aws_access_key && aws_secret_key && aws_region) {
            SPDLOG_INFO("Found all Credentials in Env Variable");
            options.ConfigureAccessKey(aws_access_key, aws_secret_key);
        }
        else {
            auto awsRoot = std::filesystem::path(getenv("HOME")) / ".aws";
            if (!std::filesystem::exists(awsRoot)) {
                throw std::runtime_error(fmt::format("Either Set all AWS ENV Variables. AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY and AWS_REGION."));
            }
            options.ConfigureDefaultCredentials();
            SPDLOG_INFO("Found all Credentials in .aws file");
        }

        if (aws_region) {
            options.region=aws_region;
        }

        // Create S3FileSystem object
        Result<std::shared_ptr<arrow::fs::FileSystem>> s3fs = arrow::fs::S3FileSystem::Make(options);
        if (!status.ok()) {
            throw std::runtime_error(fmt::format("Failed to create S3 file system:  {}.", status.ToString()));
        }
        m_s3fs = s3fs.MoveValueUnsafe();
    }

    AWSS3Reader::~AWSS3Reader() {
        const auto status =arrow::fs::FinalizeS3();
        if (!status.ok()) {
            SPDLOG_ERROR("AWS Cleanup Failed.\n{}", status.ToString());
        }
    }
}