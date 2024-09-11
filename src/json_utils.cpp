//
// Created by adesola on 6/2/24.
//
#include "json_utils.h"

/// \brief Builder that holds state for a single conversion to columnar JSON format.
///
/// Implements Visit() methods for each type of Arrow Array that set the values
/// of the corresponding fields in each column.
class ColumnBatchBuilder {
public:
    explicit ColumnBatchBuilder(int64_t num_rows, rapidjson::Document::AllocatorType& alloc) :
                                                                                               field_(nullptr),
                                                                                               allocator(alloc),
                                                                                               columns_(rapidjson::kObjectType) {}

    /// \brief Set which field to convert.
    void SetField(const arrow::Field* field) { field_ = field; }

    /// \brief Retrieve converted columns from builder.
    rapidjson::Value Result() && { return std::move(columns_); }

    // Default implementation
    arrow::Status Visit(const arrow::Array& array) {
        return arrow::Status::NotImplemented(
                "Cannot convert to json document for array of type ", array.type()->ToString());
    }

    // Handles booleans, integers, floats
    template <typename ArrayType, typename DataClass = typename ArrayType::TypeClass>
    arrow::enable_if_primitive_ctype<DataClass, arrow::Status> Visit(
            const ArrayType& array) {
        rapidjson::Value column_data(rapidjson::kArrayType);
        for (int64_t i = 0; i < array.length(); ++i) {
            if (!array.IsNull(i)) {
                column_data.PushBack(array.Value(i), allocator);
            } else {
                column_data.PushBack(rapidjson::Value(), allocator);
            }
        }

        rapidjson::Value col_name(field_->name(), allocator);
        columns_.AddMember(col_name, column_data, allocator);
        return arrow::Status::OK();
    }

    arrow::Status Visit(const arrow::StringArray& array) {
        rapidjson::Value column_data(rapidjson::kArrayType);
        for (int64_t i = 0; i < array.length(); ++i) {
            if (!array.IsNull(i)) {
                std::string_view value_view = array.Value(i);
                rapidjson::Value value;
                value.SetString(value_view.data(),
                                static_cast<rapidjson::SizeType>(value_view.size()),
                                allocator);
                column_data.PushBack(value, allocator);
            } else {
                column_data.PushBack(rapidjson::Value(), allocator);
            }
        }

        rapidjson::Value col_name(field_->name(), allocator);
        columns_.AddMember(col_name, column_data, allocator);
        return arrow::Status::OK();
    }

    arrow::Status Visit(const arrow::StructArray& array) {
        const arrow::StructType* type = array.struct_type();

        ColumnBatchBuilder child_builder(array.length(), allocator);
        for (int i = 0; i < type->num_fields(); ++i) {
            const arrow::Field* child_field = type->field(i).get();
            child_builder.SetField(child_field);
            ARROW_RETURN_NOT_OK(arrow::VisitArrayInline(*array.field(i).get(), &child_builder));
        }
        rapidjson::Value columns = std::move(child_builder).Result();

        for (int i = 0; i < type->num_fields(); ++i) {
            rapidjson::Value col_name(type->field(i)->name(), allocator);
            columns_.AddMember(col_name, columns[i], allocator);
        }

        return arrow::Status::OK();
    }

    arrow::Status Visit(const arrow::ListArray& array) {
        rapidjson::Value column_data(rapidjson::kArrayType);
        std::shared_ptr<arrow::Array> values = array.values();
        ColumnBatchBuilder child_builder(values->length(), allocator);
        const arrow::Field* value_field = array.list_type()->value_field().get();
        child_builder.SetField(value_field);
        ARROW_RETURN_NOT_OK(arrow::VisitArrayInline(*values.get(), &child_builder));

        rapidjson::Value columns = std::move(child_builder).Result();

        int64_t values_i = 0;
        for (int64_t i = 0; i < array.length(); ++i) {
            if (array.IsNull(i)) {
                column_data.PushBack(rapidjson::Value(), allocator);
                continue;
            }

            auto array_len = array.value_length(i);
            rapidjson::Value value_array(rapidjson::kArrayType);
            value_array.Reserve(array_len, allocator);

            for (int64_t j = 0; j < array_len; ++j) {
                value_array.PushBack(columns[values_i], allocator);
                ++values_i;
            }
            column_data.PushBack(value_array, allocator);
        }

        rapidjson::Value col_name(field_->name(), allocator);
        columns_.AddMember(col_name, column_data, allocator);

        return arrow::Status::OK();
    }

private:
    const arrow::Field* field_;
    rapidjson::Value columns_;
    rapidjson::Document::AllocatorType& allocator;
};  // ColumnBatchBuilder


/// \brief Builder that holds state for a single conversion.
///
/// Implements Visit() methods for each type of Arrow Array that set the values
/// of the corresponding fields in each row.
class RowBatchBuilder {
public:
    explicit RowBatchBuilder(int64_t num_rows, rapidjson::Document::AllocatorType& alloc) :
    field_(nullptr),
    allocator(alloc),
    rows_(rapidjson::kArrayType) {
        rows_.Reserve(num_rows, alloc);

        for (int64_t i = 0; i < num_rows; ++i) {
            rows_.PushBack(rapidjson::Value{rapidjson::kObjectType}, alloc);
        }
    }

    /// \brief Set which field to convert.
    void SetField(const arrow::Field* field) { field_ = field; }

    /// \brief Retrieve converted rows from builder.
    rapidjson::Value Result() && { return std::move(rows_); }

    // Default implementation
    arrow::Status Visit(const arrow::Array& array) {
        return arrow::Status::NotImplemented(
                "Cannot convert to json document for array of type ", array.type()->ToString());
    }

    // Handles booleans, integers, floats
    template <typename ArrayType, typename DataClass = typename ArrayType::TypeClass>
    arrow::enable_if_primitive_ctype<DataClass, arrow::Status> Visit(
            const ArrayType& array) {
        assert(static_cast<int64_t>(rows_.Size()) == array.length());
        for (int64_t i = 0; i < array.length(); ++i) {
            rapidjson::Value str_key(field_->name(), allocator);
            if (!array.IsNull(i)) {
                rows_[i].AddMember(str_key, array.Value(i), allocator);
            }
            else {
                rows_[i].AddMember(str_key, kNullJsonSingleton, allocator);
            }
        }

        return arrow::Status::OK();
    }

    arrow::Status Visit(const arrow::StringArray& array) {
        assert(static_cast<int64_t>(rows_.Size()) == array.length());
        for (int64_t i = 0; i < array.length(); ++i) {
            if (!array.IsNull(i)) {
                rapidjson::Value str_key(field_->name(), allocator);
                std::string_view value_view = array.Value(i);
                rapidjson::Value value;
                value.SetString(value_view.data(),
                                static_cast<rapidjson::SizeType>(value_view.size()),
                                allocator);
                rows_[i].AddMember(str_key, value, allocator);
            }
        }
        return arrow::Status::OK();
    }

    arrow::Status Visit(const arrow::StructArray& array) {
        const arrow::StructType* type = array.struct_type();

        assert(static_cast<int64_t>(rows_.Size()) == array.length());

        RowBatchBuilder child_builder(rows_.Size(), allocator);
        for (int i = 0; i < type->num_fields(); ++i) {
            const arrow::Field* child_field = type->field(i).get();
            child_builder.SetField(child_field);
            ARROW_RETURN_NOT_OK(arrow::VisitArrayInline(*array.field(i).get(), &child_builder));
        }
        rapidjson::Value rows = std::move(child_builder).Result();

        for (int64_t i = 0; i < array.length(); ++i) {
            if (!array.IsNull(i)) {
                rapidjson::Value str_key(field_->name(), allocator);
                // Must copy value to new allocator
                rapidjson::Value row_val;
                row_val.CopyFrom(rows[i], allocator);
                rows_[i].AddMember(str_key, row_val, allocator);
            }
        }
        return arrow::Status::OK();
    }

    arrow::Status Visit(const arrow::ListArray& array) {
        assert(static_cast<int64_t>(rows_.Size()) == array.length());
        // First create rows from values
        std::shared_ptr<arrow::Array> values = array.values();
        RowBatchBuilder child_builder(values->length(), allocator);
        const arrow::Field* value_field = array.list_type()->value_field().get();
        std::string value_field_name = value_field->name();
        child_builder.SetField(value_field);
        ARROW_RETURN_NOT_OK(arrow::VisitArrayInline(*values.get(), &child_builder));

        rapidjson::Value rows = std::move(child_builder).Result();

        int64_t values_i = 0;
        for (int64_t i = 0; i < array.length(); ++i) {
            if (array.IsNull(i)) continue;

            auto array_len = array.value_length(i);

            rapidjson::Value value;
            value.SetArray();
            value.Reserve(array_len, allocator);

            for (int64_t j = 0; j < array_len; ++j) {
                rapidjson::Value row_val;
                // Must copy value to new allocator
                row_val.CopyFrom(rows[values_i][value_field_name], allocator);
                value.PushBack(row_val, allocator);
                ++values_i;
            }

            rapidjson::Value str_key(field_->name(), allocator);
            rows_[i].AddMember(str_key, value, allocator);
        }

        return arrow::Status::OK();
    }

private:
    const arrow::Field* field_;
    rapidjson::Value rows_;
    rapidjson::Document::AllocatorType allocator;
};  // RowBatchBuilder

template<class BuilderType>
arrow::Result<rapidjson::Value> ConvertToVectorT(auto& batch, auto& allocator)
{
    BuilderType builder{batch->num_rows(), allocator};
    for (int i = 0; i < batch->num_columns(); ++i) {
        auto f = batch->schema()->field(i).get();
        builder.SetField(f);
        ARROW_RETURN_NOT_OK(arrow::VisitArrayInline(*batch->column(i).get(), &builder));
    }

    return std::move(builder).Result();
}

/// Convert a single batch of Arrow data into Documents
arrow::Result<rapidjson::Value> ConvertToVector(
        std::shared_ptr<arrow::RecordBatch> batch,
        bool rowFormat,
        rapidjson::Document::AllocatorType& allocator) {
    return rowFormat ? ConvertToVectorT<RowBatchBuilder>(batch, allocator) : ConvertToVectorT<ColumnBatchBuilder>(batch, allocator);
}

const rapidjson::Value* DocValuesIterator::NextArrayOrRow(const rapidjson::Value* value, size_t* path_i,
                                       int64_t* arr_i) {
    while (array_stack.size() > 0) {
        ArrayPosition& pos = array_stack.back();
        // Try to get next position in Array
        if (pos.index + 1 < pos.array_node->Size()) {
            ++pos.index;
            value = &(*pos.array_node)[pos.index];
            *path_i = pos.path_index;
            *arr_i = array_stack.size();
            return value;
        } else {
            array_stack.pop_back();
        }
    }
    ++row_i;
    if (row_i < rows.Size()) {
        value = static_cast<const rapidjson::Value*>(&rows[row_i]);
    } else {
        value = nullptr;
    }
    *path_i = 0;
    *arr_i = 0;
    return value;
}

arrow::Result<const rapidjson::Value*> DocValuesIterator::Next() {
    const rapidjson::Value* value = nullptr;
    size_t path_i;
    int64_t arr_i;
    // Can either start at document or at last array level
    if (array_stack.size() > 0) {
        auto pos = array_stack.back();
        value = pos.array_node;
        path_i = pos.path_index;
        arr_i = array_stack.size() - 1;
    }

    value = NextArrayOrRow(value, &path_i, &arr_i);

    // Traverse to desired level (with possible backtracking as needed)
    while (path_i < path.size() || arr_i < array_levels) {
        if (value == nullptr) {
            return value;
        } else if (value->IsArray() && value->Size() > 0) {
            ArrayPosition pos;
            pos.array_node = value;
            pos.path_index = path_i;
            pos.index = 0;
            array_stack.push_back(pos);

            value = &(*value)[0];
            ++arr_i;
        } else if (value->IsArray()) {
            // Empty array means we need to backtrack and go to next array or row
            value = NextArrayOrRow(value, &path_i, &arr_i);
        } else if (value->HasMember(path[path_i])) {
            value = &(*value)[path[path_i]];
            ++path_i;
        } else {
            return &kNullJsonSingleton;
        }
    }

    // Return value
    return value;
}

arrow::Status JsonValueConverter::Visit(const arrow::Int64Type& type) {
    arrow::Int64Builder* builder = static_cast<arrow::Int64Builder*>(builder_);
    for (const auto& maybe_value : FieldValues()) {
        ARROW_ASSIGN_OR_RAISE(auto value, maybe_value);
        if (value->IsNull()) {
            ARROW_RETURN_NOT_OK(builder->AppendNull());
        } else {
            if (value->IsInt64()) {
                ARROW_RETURN_NOT_OK(builder->Append(value->GetInt64()));
            }
            else if (value->IsUint()) {
                ARROW_RETURN_NOT_OK(builder->Append(value->GetUint()));
            } else if (value->IsInt()) {
                ARROW_RETURN_NOT_OK(builder->Append(value->GetInt()));
            } else if (value->IsUint64()) {
                ARROW_RETURN_NOT_OK(builder->Append(value->GetUint64()));
            } else {
                return arrow::Status::Invalid("Value is not an integer");
            }
        }
    }
    return arrow::Status::OK();
}

arrow::Status JsonValueConverter::Visit(const arrow::TimestampType& ) {
    arrow::TimestampBuilder* builder = static_cast<arrow::TimestampBuilder*>(builder_);
    for (const auto& maybe_value : FieldValues()) {
        ARROW_ASSIGN_OR_RAISE(auto value, maybe_value);
        if (value->IsNull()) {
            ARROW_RETURN_NOT_OK(builder->AppendNull());
        } else {
            if (value->IsInt64()) {
                ARROW_RETURN_NOT_OK(builder->Append(value->GetInt64()));
            } else {
                return arrow::Status::Invalid("Value is not a timestamp");
            }
        }
    }
    return arrow::Status::OK();
}

arrow::Status JsonValueConverter::Visit(const arrow::DoubleType& type) {
    arrow::DoubleBuilder* builder = static_cast<arrow::DoubleBuilder*>(builder_);
    for (const auto& maybe_value : FieldValues()) {
        ARROW_ASSIGN_OR_RAISE(auto value, maybe_value);
        if (value->IsNull()) {
            ARROW_RETURN_NOT_OK(builder->AppendNull());
        } else {
            ARROW_RETURN_NOT_OK(builder->Append(value->GetDouble()));
        }
    }
    return arrow::Status::OK();
}

arrow::Status JsonValueConverter::Visit(const arrow::StringType& type) {
    arrow::StringBuilder* builder = static_cast<arrow::StringBuilder*>(builder_);
    for (const auto& maybe_value : FieldValues()) {
        ARROW_ASSIGN_OR_RAISE(auto value, maybe_value);
        if (value->IsNull()) {
            ARROW_RETURN_NOT_OK(builder->AppendNull());
        } else {
            ARROW_RETURN_NOT_OK(builder->Append(value->GetString()));
        }
    }
    return arrow::Status::OK();
}

arrow::Status JsonValueConverter::Visit(const arrow::BooleanType& type) {
    arrow::BooleanBuilder* builder = static_cast<arrow::BooleanBuilder*>(builder_);
    for (const auto& maybe_value : FieldValues()) {
        ARROW_ASSIGN_OR_RAISE(auto value, maybe_value);
        if (value->IsNull()) {
            ARROW_RETURN_NOT_OK(builder->AppendNull());
        } else {
            ARROW_RETURN_NOT_OK(builder->Append(value->GetBool()));
        }
    }
    return arrow::Status::OK();
}

arrow::Status JsonValueConverter::Visit(const arrow::StructType& type) {
    arrow::StructBuilder* builder = static_cast<arrow::StructBuilder*>(builder_);

    std::vector<std::string> child_path(root_path_);
    if (field_name_.size() > 0) {
        child_path.push_back(field_name_);
    }
    auto child_converter = JsonValueConverter(rows_, child_path, array_levels_);

    for (int i = 0; i < type.num_fields(); ++i) {
        std::shared_ptr<arrow::Field> child_field = type.field(i);
        std::shared_ptr<arrow::ArrayBuilder> child_builder = builder->child_builder(i);

        ARROW_RETURN_NOT_OK(
                child_converter.Convert(*child_field.get(), child_builder.get()));
    }

    // Make null bitmap
    for (const auto& maybe_value : FieldValues()) {
        ARROW_ASSIGN_OR_RAISE(auto value, maybe_value);
        ARROW_RETURN_NOT_OK(builder->Append(!value->IsNull()));
    }

    return arrow::Status::OK();
}

arrow::Status JsonValueConverter::Visit(const arrow::ListType& type) {
    arrow::ListBuilder* builder = static_cast<arrow::ListBuilder*>(builder_);

    // Values and offsets needs to be interleaved in ListBuilder, so first collect the
    // values
    std::unique_ptr<arrow::ArrayBuilder> tmp_value_builder;
    ARROW_ASSIGN_OR_RAISE(tmp_value_builder,
                          arrow::MakeBuilder(builder->value_builder()->type()));
    std::vector<std::string> child_path(root_path_);
    child_path.push_back(field_name_);
    auto child_converter = JsonValueConverter(rows_, child_path, array_levels_ + 1);
    ARROW_RETURN_NOT_OK(
            child_converter.Convert(*type.value_field().get(), "", tmp_value_builder.get()));

    std::shared_ptr<arrow::Array> values_array;
    ARROW_RETURN_NOT_OK(tmp_value_builder->Finish(&values_array));
    std::shared_ptr<arrow::ArrayData> values_data = values_array->data();

    arrow::ArrayBuilder* value_builder = builder->value_builder();
    int64_t offset = 0;
    for (const auto& maybe_value : FieldValues()) {
        ARROW_ASSIGN_OR_RAISE(auto value, maybe_value);
        ARROW_RETURN_NOT_OK(builder->Append(!value->IsNull()));
        if (!value->IsNull() && value->Size() > 0) {
            ARROW_RETURN_NOT_OK(
                    value_builder->AppendArraySlice(*values_data.get(), offset, value->Size()));
            offset += value->Size();
        }
    }

    return arrow::Status::OK();
}

arrow::Iterator<const rapidjson::Value*> JsonValueConverter::FieldValues() {
    std::vector<std::string> path(root_path_);
    if (field_name_.size() > 0) {
        path.push_back(field_name_);
    }
    auto iter = DocValuesIterator(rows_, std::move(path), array_levels_);
    auto fn = [iter]() mutable -> arrow::Result<const rapidjson::Value*> {
        return iter.Next();
    };

    return arrow::MakeFunctionIterator(fn);
}

arrow::Status JsonColumnConverter::Visit(const arrow::Int64Type& type) {
    arrow::Int64Builder* builder = static_cast<arrow::Int64Builder*>(builder_);
    const auto& column = columns_[field_name_.c_str()];
    for (rapidjson::SizeType i = 0; i < column.Size(); ++i) {
        if (column[i].IsNull()) {
            ARROW_RETURN_NOT_OK(builder->AppendNull());
        } else {
            ARROW_RETURN_NOT_OK(builder->Append(column[i].GetInt64()));
        }
    }
    return arrow::Status::OK();
}

arrow::Status JsonColumnConverter::Visit(const arrow::TimestampType& type) {
    auto builder = static_cast<arrow::TimestampBuilder*>(builder_);
    const auto& column = columns_[field_name_.c_str()];
    for (rapidjson::SizeType i = 0; i < column.Size(); ++i) {
        if (column[i].IsNull()) {
            ARROW_RETURN_NOT_OK(builder->AppendNull());
        } else {
            ARROW_RETURN_NOT_OK(builder->Append(column[i].GetInt64()));
        }
    }
    return arrow::Status::OK();
}

arrow::Status JsonColumnConverter::Visit(const arrow::DoubleType& type) {
    arrow::DoubleBuilder* builder = static_cast<arrow::DoubleBuilder*>(builder_);
    const auto& column = columns_[field_name_.c_str()];
    for (rapidjson::SizeType i = 0; i < column.Size(); ++i) {
        if (column[i].IsNull()) {
            ARROW_RETURN_NOT_OK(builder->AppendNull());
        } else {
            ARROW_RETURN_NOT_OK(builder->Append(column[i].GetDouble()));
        }
    }
    return arrow::Status::OK();
}

arrow::Status JsonColumnConverter::Visit(const arrow::StringType& type) {
    arrow::StringBuilder* builder = static_cast<arrow::StringBuilder*>(builder_);
    const auto& column = columns_[field_name_.c_str()];
    for (rapidjson::SizeType i = 0; i < column.Size(); ++i) {
        if (column[i].IsNull()) {
            ARROW_RETURN_NOT_OK(builder->AppendNull());
        } else {
            ARROW_RETURN_NOT_OK(builder->Append(column[i].GetString()));
        }
    }
    return arrow::Status::OK();
}

arrow::Status JsonColumnConverter::Visit(const arrow::BooleanType& type) {
    arrow::BooleanBuilder* builder = static_cast<arrow::BooleanBuilder*>(builder_);
    const auto& column = columns_[field_name_.c_str()];
    for (rapidjson::SizeType i = 0; i < column.Size(); ++i) {
        if (column[i].IsNull()) {
            ARROW_RETURN_NOT_OK(builder->AppendNull());
        } else {
            ARROW_RETURN_NOT_OK(builder->Append(column[i].GetBool()));
        }
    }
    return arrow::Status::OK();
}

arrow::Status JsonColumnConverter::Visit(const arrow::StructType& type) {
    arrow::StructBuilder* builder = static_cast<arrow::StructBuilder*>(builder_);
    for (int i = 0; i < type.num_fields(); ++i) {
        std::shared_ptr<arrow::Field> child_field = type.field(i);
        std::shared_ptr<arrow::ArrayBuilder> child_builder = builder->child_builder(i);

        JsonColumnConverter child_converter(columns_);
        ARROW_RETURN_NOT_OK(child_converter.Convert(*child_field.get(), child_builder.get()));
    }

    for (rapidjson::SizeType i = 0; i < columns_[field_name_.c_str()].Size(); ++i) {
        ARROW_RETURN_NOT_OK(builder->Append(!columns_[field_name_.c_str()][i].IsNull()));
    }

    return arrow::Status::OK();
}

arrow::Status JsonColumnConverter::Visit(const arrow::ListType& type) {
    arrow::ListBuilder* builder = static_cast<arrow::ListBuilder*>(builder_);
    const auto& column = columns_[field_name_.c_str()];

    // Values and offsets need to be interleaved in ListBuilder, so first collect the values
    std::unique_ptr<arrow::ArrayBuilder> tmp_value_builder;
    ARROW_ASSIGN_OR_RAISE(tmp_value_builder,
                          arrow::MakeBuilder(builder->value_builder()->type()));

    JsonColumnConverter child_converter(columns_);
    ARROW_RETURN_NOT_OK(child_converter.Convert(*type.value_field().get(), "", tmp_value_builder.get()));

    std::shared_ptr<arrow::Array> values_array;
    ARROW_RETURN_NOT_OK(tmp_value_builder->Finish(&values_array));
    std::shared_ptr<arrow::ArrayData> values_data = values_array->data();

    arrow::ArrayBuilder* value_builder = builder->value_builder();
    int64_t offset = 0;
    for (rapidjson::SizeType i = 0; i < column.Size(); ++i) {
        ARROW_RETURN_NOT_OK(builder->Append(!column[i].IsNull()));
        if (!column[i].IsNull() && column[i].Size() > 0) {
            ARROW_RETURN_NOT_OK(
                    value_builder->AppendArraySlice(*values_data.get(), offset, column[i].Size()));
            offset += column[i].Size();
        }
    }

    return arrow::Status::OK();
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> ConvertToRecordBatch(
        const rapidjson::Document& doc, std::shared_ptr<arrow::Schema> schema) {
    // Check if the JSON document is columnar or row-wise
    bool is_columnar = doc.IsObject();
    for (auto it = doc.MemberBegin(); it != doc.MemberEnd(); ++it) {
        if (it->value.IsArray()) {
            is_columnar = true;
        } else {
            is_columnar = false;
            break;
        }
    }

    // Create a RecordBatchBuilder
    std::unique_ptr<arrow::RecordBatchBuilder> batch_builder;
    if (is_columnar) {
        ARROW_ASSIGN_OR_RAISE(
                batch_builder,
                arrow::RecordBatchBuilder::Make(schema, arrow::default_memory_pool(), doc.MemberBegin()->value.Size()));
    } else {
        ARROW_ASSIGN_OR_RAISE(
                batch_builder,
                arrow::RecordBatchBuilder::Make(schema, arrow::default_memory_pool(), doc.GetArray().Size()));
    }

    // Use appropriate converter based on the JSON structure
    if (is_columnar) {
        JsonColumnConverter converter(doc);
        for (int i = 0; i < batch_builder->num_fields(); ++i) {
            std::shared_ptr<arrow::Field> field = schema->field(i);
            arrow::ArrayBuilder* builder = batch_builder->GetField(i);
            ARROW_RETURN_NOT_OK(converter.Convert(*field.get(), builder));
        }
    } else {
        JsonValueConverter converter(doc.GetArray());
        for (int i = 0; i < batch_builder->num_fields(); ++i) {
            std::shared_ptr<arrow::Field> field = schema->field(i);
            arrow::ArrayBuilder* builder = batch_builder->GetField(i);
            ARROW_RETURN_NOT_OK(converter.Convert(*field.get(), builder));
        }
    }

    std::shared_ptr<arrow::RecordBatch> batch;
    ARROW_ASSIGN_OR_RAISE(batch, batch_builder->Flush());

    // Use RecordBatch::ValidateFull() to make sure arrays were correctly constructed.
    DCHECK_OK(batch->ValidateFull());
    return batch;
}  // ConvertToRecordBatch