// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.
#pragma once


#include <arrow/api.h>
#include <arrow/result.h>
#include <arrow/table_builder.h>
#include <arrow/type_traits.h>
#include <arrow/util/iterator.h>
#include <arrow/util/logging.h>
#include <arrow/visit_array_inline.h>

#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include <cassert>
#include <iostream>
#include <vector>
#include <spdlog/spdlog.h>
// Transforming dynamic row data into Arrow data
// When building connectors to other data systems, it's common to receive data in
// row-based structures. While the row_wise_conversion_example.cc shows how to
// handle this conversion for fixed schemas, this example demonstrates how to
// writer converters for arbitrary schemas.
//
// As an example, this conversion is between Arrow and rapidjson::Documents.
//
// We use the following helpers and patterns here:
//  * arrow::VisitArrayInline and arrow::VisitTypeInline for implementing a visitor
//    pattern with Arrow to handle different array types
//  * arrow::enable_if_primitive_ctype to create a template method that handles
//    conversion for Arrow types that have corresponding C types (bool, integer,
//    float).

static rapidjson::Value kNullJsonSingleton = rapidjson::Value();


arrow::Result<rapidjson::Value> ConvertToVector(
        std::shared_ptr<arrow::RecordBatch> batch,
        bool rowFormat,
        rapidjson::Document::AllocatorType& allocator);

/// \brief Iterator over rows values of a document for a given field
///
/// path and array_levels are used to address each field in a JSON document. As
/// an example, consider this JSON document:
/// {
///     "x": 3,                   // path: ["x"],             array_levels: 0
///     "files": [                // path: ["files"],         array_levels: 0
///         {                     // path: ["files"],         array_levels: 1
///             "path": "my_str", // path: ["files", "path"], array_levels: 1
///             "sizes": [        // path: ["files", "size"], array_levels: 1
///                 20,           // path: ["files", "size"], array_levels: 2
///                 22
///             ]
///         }
///     ]
/// },
class DocValuesIterator {
public:
    /// \param rows vector of rows
    /// \param path field names to enter
    /// \param array_levels number of arrays to enter
    DocValuesIterator(const rapidjson::GenericArray<true, rapidjson::Value> &rows,
                      std::vector<std::string> path, int64_t array_levels)
            : rows(rows), path(std::move(path)), array_levels(array_levels) {}

    const rapidjson::Value *NextArrayOrRow(const rapidjson::Value *value, size_t *path_i,
                                           int64_t *arr_i);

    arrow::Result<const rapidjson::Value *> Next();

private:
    const rapidjson::GenericArray<true, rapidjson::Value> &rows;
    std::vector<std::string> path;
    int64_t array_levels;
    size_t row_i = -1;  // index of current row

    // Info about array position for one array level in array stack
    struct ArrayPosition {
        const rapidjson::Value *array_node;
        int64_t path_index;
        rapidjson::SizeType index;
    };
    std::vector<ArrayPosition> array_stack;
};

class JsonValueConverter {
public:
    explicit JsonValueConverter(const rapidjson::GenericArray<true, rapidjson::Value>& rows)
            : rows_(rows), array_levels_(0) {}

    JsonValueConverter(const rapidjson::GenericArray<true, rapidjson::Value>& rows,
                       const std::vector<std::string>& root_path, int64_t array_levels)
            : rows_(rows), root_path_(root_path), array_levels_(array_levels) {}

    /// \brief For field passed in, append corresponding values to builder
    arrow::Status Convert(const arrow::Field& field, arrow::ArrayBuilder* builder) {
        return Convert(field, field.name(), builder);
    }

    /// \brief For field passed in, append corresponding values to builder
    arrow::Status Convert(const arrow::Field& field, const std::string& field_name,
                          arrow::ArrayBuilder* builder) {
        field_name_ = field_name;
        builder_ = builder;
        ARROW_RETURN_NOT_OK(arrow::VisitTypeInline(*field.type().get(), this));
        return arrow::Status::OK();
    }

    // Default implementation
    arrow::Status Visit(const arrow::DataType& type) {
        return arrow::Status::NotImplemented(
                "Cannot convert json value to Arrow array of type ", type.ToString());
    }

    arrow::Status Visit(const arrow::Int64Type& type);

    arrow::Status Visit(const arrow::TimestampType& type);

    arrow::Status Visit(const arrow::DoubleType& type);

    arrow::Status Visit(const arrow::StringType& type);

    arrow::Status Visit(const arrow::BooleanType& type);

    arrow::Status Visit(const arrow::StructType& type);

    arrow::Status Visit(const arrow::ListType& type);

private:
    std::string field_name_;
    arrow::ArrayBuilder* builder_;
    const rapidjson::GenericArray<true, rapidjson::Value> rows_;
    std::vector<std::string> root_path_;
    int64_t array_levels_;

    /// Return a flattened iterator over values at nested location
    arrow::Iterator<const rapidjson::Value*> FieldValues();
};  // JsonValueConverter

/// \brief Converter that holds state for converting columnar JSON to Arrow Arrays.
class JsonColumnConverter {
public:
    explicit JsonColumnConverter(const rapidjson::Value& columns)
        : columns_(columns) {}

    /// \brief For field passed in, append corresponding values to builder
    arrow::Status Convert(const arrow::Field& field, arrow::ArrayBuilder* builder) {
        return Convert(field, field.name(), builder);
    }

    /// \brief For field passed in, append corresponding values to builder
    arrow::Status Convert(const arrow::Field& field, const std::string& field_name,
                          arrow::ArrayBuilder* builder) {
        field_name_ = field_name;
        builder_ = builder;
        ARROW_RETURN_NOT_OK(arrow::VisitTypeInline(*field.type().get(), this));
        return arrow::Status::OK();
    }

    // Default implementation
    arrow::Status Visit(const arrow::DataType& type) {
        return arrow::Status::NotImplemented(
                "Cannot convert json value to Arrow array of type ", type.ToString());
    }

    arrow::Status Visit(const arrow::Int64Type& type);

    arrow::Status Visit(const arrow::TimestampType& type);

    arrow::Status Visit(const arrow::DoubleType& type);

    arrow::Status Visit(const arrow::StringType& type);

    arrow::Status Visit(const arrow::BooleanType& type);

    arrow::Status Visit(const arrow::StructType& type);

    arrow::Status Visit(const arrow::ListType& type);

private:
    std::string field_name_;
    arrow::ArrayBuilder* builder_;
    const rapidjson::Value& columns_;

    /// Return a flattened iterator over values at nested location
    arrow::Iterator<const rapidjson::Value*> ColumnValues();
};  // JsonColumnConverter

arrow::Result<std::shared_ptr<arrow::RecordBatch>> ConvertToRecordBatch(
        const rapidjson::Document& doc, std::shared_ptr<arrow::Schema> schema);