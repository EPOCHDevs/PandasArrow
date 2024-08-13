//
// Created by adesola on 8/13/24.
//
#include "list_s3_files.h"
#include "aws_s3_reader.h"
#include <aws/core/Aws.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/s3/model/ListObjectsV2Result.h>
#include <spdlog/spdlog.h>


namespace pd {
    std::vector<std::string> ListS3Paths(std::string folder) {
        arrow::AWSS3Reader::Instance();
        // Set up the S3 client
        Aws::S3::S3Client s3_client;

        // Specify your bucket name and the folder (prefix) you want to query

        folder.replace(folder.find("s3://"), 5, "");

        auto first_sep = folder.find("/");
        const Aws::String bucket_name = folder.substr(0, first_sep);
        const Aws::String folder_name = folder.substr(first_sep + 1);

        // Set up the request
        Aws::S3::Model::ListObjectsV2Request request;
        request.SetBucket(bucket_name);
        request.SetPrefix(folder_name);

        // Send the request
        auto outcome = s3_client.ListObjectsV2(request);
        std::vector<std::string> result;
        if (outcome.IsSuccess()) {
            const auto &objects = outcome.GetResult().GetContents();
            result.reserve(objects.size());
            std::ranges::transform(objects, std::back_inserter(result), [&](auto const &object) {
                return fmt::format("s3://{}/{}", bucket_name, object.GetKey());
            });
        } else {
            SPDLOG_ERROR(outcome.GetError().GetMessage());
        }
        return result;
    }
}