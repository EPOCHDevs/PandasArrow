//
// Created by adesola on 6/2/24.
//
#pragma once

#include <arrow/api.h>
#include <rapidjson/document.h>
#include <iostream>


namespace pd {
    template <typename ArrayType, typename ValueType>
    rapidjson::Value ConvertToJsonArray(const std::shared_ptr<ArrayType>& array, int64_t row, rapidjson::Document::AllocatorType& allocator) {
        rapidjson::Value value;
        if (!array->IsNull(row)) {
            value.Set<ValueType>(array->Value(row));
        } else {
            value.SetNull();
        }
        return value;
    }

    rapidjson::Value ConvertStringArrayToJson(const std::shared_ptr<arrow::StringArray>& array, int64_t row, rapidjson::Document::AllocatorType& allocator);

    rapidjson::Value ConvertBooleanArrayToJson(const std::shared_ptr<arrow::BooleanArray>& array, int64_t row, rapidjson::Document::AllocatorType& allocator);

    rapidjson::Value ConvertArrayToJson(const std::shared_ptr<arrow::Array>& array, int64_t row, rapidjson::Document::AllocatorType& allocator);
}
