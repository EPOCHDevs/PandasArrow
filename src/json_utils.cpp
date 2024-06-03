//
// Created by adesola on 6/2/24.
//
#include "json_utils.h"

#include <spdlog/spdlog.h>

namespace pd{

    rapidjson::Value ConvertStringArrayToJson(const std::shared_ptr<arrow::StringArray>& array, int64_t row, rapidjson::Document::AllocatorType& allocator) {
        rapidjson::Value value;
        if (!array->IsNull(row)) {
            value.SetString(array->GetString(row).c_str(), allocator);
        } else {
            value.SetNull();
        }
        return value;
    }

    rapidjson::Value ConvertBooleanArrayToJson(const std::shared_ptr<arrow::BooleanArray>& array, int64_t row, rapidjson::Document::AllocatorType& allocator) {
        rapidjson::Value value;
        if (!array->IsNull(row)) {
            value.SetBool(array->Value(row));
        } else {
            value.SetNull();
        }
        return value;
    }

    rapidjson::Value ConvertArrayToJson(const std::shared_ptr<arrow::Array>& array, int64_t row, rapidjson::Document::AllocatorType& allocator) {
        switch (array->type_id()) {
            case arrow::Type::STRING:
                return ConvertStringArrayToJson(std::static_pointer_cast<arrow::StringArray>(array), row, allocator);
            case arrow::Type::UINT64:
                return ConvertToJsonArray<arrow::UInt64Array, uint64_t>(std::static_pointer_cast<arrow::UInt64Array>(array), row, allocator);
            case arrow::Type::UINT32:
                return ConvertToJsonArray<arrow::UInt32Array, uint32_t>(std::static_pointer_cast<arrow::UInt32Array>(array), row, allocator);
            case arrow::Type::INT64:
                return ConvertToJsonArray<arrow::Int64Array, int64_t>(std::static_pointer_cast<arrow::Int64Array>(array), row, allocator);
            case arrow::Type::INT32:
                return ConvertToJsonArray<arrow::Int32Array, int32_t>(std::static_pointer_cast<arrow::Int32Array>(array), row, allocator);
            case arrow::Type::FLOAT:
                return ConvertToJsonArray<arrow::FloatArray, float>(std::static_pointer_cast<arrow::FloatArray>(array), row, allocator);
            case arrow::Type::DOUBLE:
                return ConvertToJsonArray<arrow::DoubleArray, double>(std::static_pointer_cast<arrow::DoubleArray>(array), row, allocator);
            case arrow::Type::BOOL:
                return ConvertBooleanArrayToJson(std::static_pointer_cast<arrow::BooleanArray>(array), row, allocator);
            // Add more type conversions as needed
            default:
                spdlog::error("Unsupported Arrow type({}) for conversion to JSON", array->type()->ToString());
                throw;
        }
    }
}
