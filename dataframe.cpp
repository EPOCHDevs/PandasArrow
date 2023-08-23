//
// Created by dewe on 12/27/22.
//
#include "dataframe.h"
#include <arrow/api.h>
#include <arrow/compute/exec.h>
#include <arrow/io/api.h>
#include "arrow/csv/api.h"
#include <iostream>
#include <oneapi/tbb/parallel_for_each.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include "arrow/array.h"
#include "arrow/array/concatenate.h"
#include "arrow/chunked_array.h"
#include "arrow/compute/api_aggregate.h"
#include "arrow/compute/api_vector.h"
#include "arrow/compute/cast.h"
//#include "arrow/compute/exec/aggregate.h"
#include "arrow/compute/registry.h"
#include "arrow/table.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/int_util_overflow.h"
#include "arrow/util/key_value_metadata.h"
#include "arrow/util/vector.h"
#include "datetimelike.h"
#include "filesystem"
#include "macros.h"
#include "resample.h"

namespace pd {

DataFrame::DataFrame(const std::shared_ptr<arrow::RecordBatch>& table, const std::shared_ptr<arrow::Array>& _index)
    : NDFrame(table, _index)
{
    if (not _index)
    {
        if (table)
        {
            m_index = uint_range(table->num_rows());
        }
    }
}

DataFrame::DataFrame(const ArrayTable& table, const shared_ptr<arrow::Array>& index)
    : NDFrame(GetTableRowSize(table), index)
{
//    if (m_index->length() == 0)
//    {
//        throw std::runtime_error("Cannot Create DataFrame with empty columns");
//    }

    int nRows = m_index->length();

    arrow::FieldVector fieldVectors;
    fieldVectors.reserve(table.size());

    arrow::ArrayDataVector arrayVector;
    arrayVector.reserve(table.size());

    for (auto const& [key, value] : table)
    {
        fieldVectors.emplace_back(arrow::field(key, value->type()));
        arrayVector.emplace_back(value->data());
    }

    m_array = arrow::RecordBatch::Make(arrow::schema(fieldVectors), nRows, std::move(arrayVector));
}


bool DataFrame::contains(std::string const& column) const
{
    return m_array->schema()->GetFieldIndex(column) != -1;
}

DataFrame::DataFrame(std::shared_ptr<arrow::StructArray> const& arr, std::vector<std::string> const& columns = {})
    : NDFrame<DataFrame>(nullptr)
{

    arrow::FieldVector fields;
    arrow::ArrayVector arrayVector;

    if (columns.empty())
    {
        arrayVector = arr->fields();
        auto names = makeDefaultColumnNames(arrayVector.size());
        for (auto const& name : names)
        {
            fields.emplace_back(arrow::field(name, arr->type()));
        }
    }
    else
    {
        for (auto const& column : columns)
        {
            auto array = arr->GetFieldByName(column);
            fields.emplace_back(arrow::field(column, arr->type()));
            arrayVector.emplace_back(array);
        }
    }

    int num_rows = arrayVector.back()->length();
    m_index = uint_range(num_rows);
    m_array = arrow::RecordBatch::Make(arrow::schema(fields), num_rows, arrayVector);
}

DataFrame& DataFrame::rename(std::unordered_map<std::string, std::string> const& columns)
{
    auto schemas = m_array->schema();

    for (auto const& [old, new_] : columns)
    {
        auto index = schemas->GetFieldIndex(old);
        if (index != -1)
        {
            auto field = schemas->GetFieldByName(old)->WithName(new_);
            schemas = schemas->SetField(index, field).MoveValueUnsafe();
        }
        else
        {
            throw std::runtime_error("trying to rename with invalid field: " + old);
        }
    }
    m_array = arrow::RecordBatch::Make(schemas, num_rows(), m_array->columns());
    return *this;
}

void DataFrame::add_prefix(std::string const& prefix, std::set<std::string> const& excludes)
{
    std::unordered_map<std::string, std::string> new_columns;
    for (auto const& name : m_array->schema()->field_names())
    {
        if (not excludes.contains(name))
        {
            new_columns[name] = prefix + name;
        }
    }
    rename(new_columns);
}

void DataFrame::add_suffix(std::string const& suffix, std::set<std::string> const& excludes)
{
    std::unordered_map<std::string, std::string> new_columns;
    for (auto const& name : m_array->schema()->field_names())
    {
        if (not excludes.contains(name))
        {
            new_columns[name] = name + suffix;
        }
    }
    rename(new_columns);
}

std::vector<std::string> DataFrame::makeDefaultColumnNames(size_t N)
{
    std::vector<std::string> columns(N);
    for (size_t i = 0; i < N; i++)
    {
        columns[i] = std::to_string(i);
    }
    return columns;
}

Series DataFrame::index() const
{
    return { m_index, true, "index" };
}

DataFrame DataFrame::setIndex(std::string const& column_name)
{
    auto column_idx = m_array->schema()->GetFieldIndex(column_name);
    if (column_idx == -1)
    {
        throw std::runtime_error(column_name + " is not in the columns");
    }

    auto new_index = m_array->GetColumnByName(column_name);
    return { pd::ReturnOrThrowOnFailure(m_array->RemoveColumn(column_idx)), new_index };
}

DataFrame DataFrame::indexAsDateTime() const
{
    return setIndex(pd::Series(m_index, true, "index").to_datetime().array());
}

DataFrame DataFrame::setColumns(std::vector<std::string> const& column_names)
{
    auto current_column_names = columnNames();
    std::unordered_map<std::string, std::string> replace;
    for (int i = 0; i < column_names.size(); ++i)
    {
        replace[current_column_names[i]] = column_names[i];
    }
    return rename(replace);
}

DataFrame DataFrame::readParquet(std::filesystem::path const& path)
{
    auto&& infileStatus = arrow::io::ReadableFile::Open(path);
    if (infileStatus.ok())
    {
        auto infile = std::move(infileStatus).ValueUnsafe();

        std::unique_ptr<parquet::arrow::FileReader> reader;
        // Open up the file with the IPC features of the library, gives us a reader object.
        PARQUET_THROW_NOT_OK(parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader));

        std::shared_ptr<arrow::Table> parquet_table;
        PARQUET_THROW_NOT_OK(reader->ReadTable(&parquet_table));

        arrow::TableBatchReader tableBatchReader(parquet_table);
        auto recordBatchesResult = tableBatchReader.ToRecordBatches();

        if (recordBatchesResult.ok())
        {
            auto rb = recordBatchesResult.MoveValueUnsafe();
            if (rb.size() == 1)
            {
                return rb.at(0);
            }
            else if (rb.empty())
            {
                throw std::runtime_error("Cannot Initialize DataFrame with empty parquet table");
            }
            else
            {
                std::stringstream ss;
                ss << "DataFrame Only supports Parquet Table with single record batch\n"
                      "Found "
                   << rb.size() << " record batches\n";
                throw std::runtime_error(ss.str());
            }
        }
        else
        {
            throw std::runtime_error(recordBatchesResult.status().ToString());
        }
    }
    else
    {
        throw std::runtime_error(infileStatus.status().ToString());
    }
}


DataFrame DataFrame::readCSV(std::filesystem::path const& path)
{
    arrow::io::IOContext io_context = arrow::io::default_io_context();
    std::shared_ptr<arrow::io::InputStream> input =
        pd::ReturnOrThrowOnFailure(arrow::io::ReadableFile::Open(path));

    auto read_options = arrow::csv::ReadOptions::Defaults();
    auto parse_options = arrow::csv::ParseOptions::Defaults();
    auto convert_options = arrow::csv::ConvertOptions::Defaults();

    std::shared_ptr<arrow::csv::TableReader> csv_reader = pd::ReturnOrThrowOnFailure(arrow::csv::TableReader::Make(io_context, input, read_options, parse_options, convert_options));
    std::shared_ptr<arrow::Table> table = pd::ReturnOrThrowOnFailure(csv_reader->Read());
    arrow::TableBatchReader tableBatchReader(table);

    return pd::ReturnOrThrowOnFailure(tableBatchReader.ToRecordBatches()).at(0);
}

std::ostream& operator<<(std::ostream& os, DataFrame const& df)
{
    tabulate::Table table;
    if (not df.m_array)
    {
        return os;
    }

    int64_t nRows = df.m_array->num_rows();
    int64_t nCols = df.m_array->num_columns();

    const auto halfRow = int(PD_MAX_ROW_TO_PRINT / 2);
    const auto halfCol = int(PD_MAX_COL_TO_PRINT / 2);

    tabulate::Table::Row_t cells, dot{ halfRow, "" };
    cells.emplace_back("index");
    for (int j = 0; j < nCols; j++)
    {
        if (nCols <= PD_MAX_COL_TO_PRINT or (nCols > PD_MAX_COL_TO_PRINT and (j < halfCol or j > nCols - halfCol)))
        {
            cells.emplace_back(df.m_array->schema()->field(j)->name());
        }
    }
    table.add_row(cells);

    bool added_sep = false;
    for (int64_t i = 0; i < nRows; i++)
    {
        if (nRows <= PD_MAX_ROW_TO_PRINT or (nRows > PD_MAX_ROW_TO_PRINT and (i < halfRow or i > (nRows - halfRow))))
        {
            cells.clear();
            cells.emplace_back(df.m_index->GetScalar(i).MoveValueUnsafe()->ToString());
            for (int j = 0; j < nCols; j++)
            {
                if (nCols <= PD_MAX_COL_TO_PRINT or
                    (nCols > PD_MAX_COL_TO_PRINT and (j < halfCol or j > nCols - halfCol)))
                {
                    cells.emplace_back(df.m_array->column(j)->GetScalar(i).MoveValueUnsafe()->ToString());
                }
            }
            table.add_row(cells);
        }
        else if (not added_sep)
        {
            table.add_row(dot);
            table.add_row(dot);
            table.add_row(dot);
            added_sep = true;
        }
    }
    os << table;
    return os;
}

Series DataFrame::operator[](const std::string& column) const
{
    auto ptr = m_array->GetColumnByName(column);
    if (ptr)
    {
        return { ptr, m_index, column };
    }
    auto columns = columnNames();
    std::stringstream ss;
    ss << column + " is not a valid column, Valid Columns: ";
    for (auto const& c : columns)
    {
        ss << c << " ";
    }
    throw std::runtime_error(ss.str());
}

DataFrame DataFrame::operator[](int64_t row) const
{
    std::map<std::string, pd::ArrayPtr> result;
    auto columns = columnNames();
    std::transform(
        columns.begin(),
        columns.end(),
        std::inserter(result, std::end(result)),
        [this, row](std::string const& column)
        {
            auto series = this->m_array->GetColumnByName(column);
            if (series)
            {
                auto scalar = pd::ReturnOrThrowOnFailure(series->GetScalar(row));
                return std::pair{ column, pd::ReturnOrThrowOnFailure(arrow::MakeArrayFromScalar(*scalar, 1)) };
            }
            std::stringstream ss;
            ss << "[" << column << "] returned null.";
            throw std::runtime_error(ss.str());
        });
    auto index = pd::ReturnOrThrowOnFailure(m_index->GetScalar(row));
    return DataFrame{ result, pd::ReturnOrThrowOnFailure(arrow::MakeArrayFromScalar(*index, 1)) };
}

DataFrame DateTimeLike::iso_calendar() const
{
    auto result = pd::ReturnOrThrowOnFailure(arrow::compute::ISOCalendar(m_array));
    return { result.array_as<arrow::StructArray>(),
             std::vector<std::string>{ "iso_year", "iso_week", "iso_day_of_week" } };
}

DataFrame DateTimeLike::year_month_day() const
{
    auto result = pd::ReturnOrThrowOnFailure(arrow::compute::ISOCalendar(m_array));
    return { result.array_as<arrow::StructArray>(), std::vector<std::string>{ "year", "month", "day" } };
}

Scalar DataFrame::at(int64_t row, int64_t col) const
{
    if (row < 0)
    {
        throw std::runtime_error("@DataFrame::at() row must be >= 0");
    }

    if (col < 0)
    {
        throw std::runtime_error("@DataFrame::at() col must be >= 0");
    }

    if (col < num_columns())
    {
        auto ptr = m_array->column(col);
        auto result = ptr->GetScalar(row);
        if (result.ok())
        {
            return result.MoveValueUnsafe();
        }
        throw std::runtime_error(std::to_string(row) + " is an invalid row index for column " + columnNames().at(col));
    }

    throw std::runtime_error(std::to_string(col) + " is not a valid column index");
}

std::vector<std::string> DataFrame::columnNames() const
{
    return m_array->schema()->field_names();
}

DataFrame DataFrame::slice(DateTimeSlice slicer, const std::vector<std::string>& columns) const
{
    if (m_index->type_id() == arrow::Type::TIMESTAMP)
    {
        int64_t start = 0, end = m_index->length() - 1;

        if (slicer.start)
        {
            start = ReturnScalarOrThrowOnError<int64_t>(
                arrow::compute::Index(m_index, arrow::compute::IndexOptions{ fromDateTime(slicer.start.value()) }));
        }
        if (slicer.end)
        {
            end = ReturnScalarOrThrowOnError<int64_t>(
                arrow::compute::Index(m_index, arrow::compute::IndexOptions{ fromDateTime(slicer.end.value()) }));
        }

        return slice(Slice{ start, end }, columns);
    }
    else
    {
        std::stringstream ss;
        ss << "Type Error: DateTime slicing is only allowed on TimeStamp DataType, but found index of type "
           << m_index->type()->ToString() << "\n";
        throw std::runtime_error(ss.str());
    }
}

DataFrame DataFrame::slice(Slice slice, std::vector<std::string> const& columns) const
{
    slice.normalize(m_array->num_rows());

    int64_t length = slice.end - slice.start;

    std::vector<std::shared_ptr<arrow::ArrayData>> arrays;
    arrays.reserve(columns.size());

    arrow::FieldVector fieldVector;
    fieldVector.reserve(columns.size());

    auto schema = m_array->schema();

    for (auto const& column : columns)
    {
        auto idx = schema->GetFieldIndex(column);
        if (idx == -1)
        {
            throw std::runtime_error("Invalid column: " + column);
        }
        arrays.emplace_back(m_array->column(idx)->data()->Slice(slice.start, length));
        fieldVector.emplace_back(schema->field(idx));
    }
    return { arrow::RecordBatch::Make(arrow::schema(fieldVector), length, arrays),
             m_index->Slice(slice.start, length) };
}

DataFrame DataFrame::slice(Slice slice) const
{
    slice.normalize(m_array->num_rows());
    int64_t length = slice.end - slice.start;

    std::vector<std::shared_ptr<arrow::ArrayData>> arrays;
    arrays.reserve(m_array->num_columns());

    auto schema = m_array->schema();

    for (auto const& column : m_array->columns())
    {
        arrays.emplace_back(column->data()->Slice(slice.start, length));
    }
    return { arrow::RecordBatch::Make(schema, length, arrays), m_index->Slice(slice.start, length) };
}

DataFrame DataFrame::DataFrame::slice(DateTimeSlice slicer) const
{
    if (m_index->type_id() == arrow::Type::TIMESTAMP)
    {
        int64_t start = 0, end = m_index->length() - 1;

        if (slicer.start)
        {
            start = ReturnScalarOrThrowOnError<int64_t>(
                arrow::compute::Index(m_index, arrow::compute::IndexOptions{ fromDateTime(slicer.start.value()) }));
        }
        if (slicer.end)
        {
            end = ReturnScalarOrThrowOnError<int64_t>(
                arrow::compute::Index(m_index, arrow::compute::IndexOptions{ fromDateTime(slicer.end.value()) }));
        }

        return slice(Slice{ start, end });
    }
    else
    {
        std::stringstream ss;
        ss << "Type Error: DateTime slicing is only allowed on TimeStamp DataType, but found index of type "
           << m_index->type()->ToString() << "\n";
        throw std::runtime_error(ss.str());
    }
}

DataFrame DataFrame::slice(int offset, std::vector<std::string> const& columns) const
{
    int64_t length = m_array->num_rows() - offset;
    std::vector<std::shared_ptr<arrow::ArrayData>> arrays;
    arrays.reserve(columns.size());

    arrow::FieldVector fieldVector;
    fieldVector.reserve(columns.size());

    auto schema = m_array->schema();

    for (auto const& column : columns)
    {
        auto idx = schema->GetFieldIndex(column);
        if (idx == -1)
        {
            throw std::runtime_error("Invalid column: " + column);
        }
        arrays.emplace_back(m_array->column(idx)->data()->Slice(offset, length));
        fieldVector.emplace_back(schema->field(idx));
    }
    return { arrow::RecordBatch::Make(arrow::schema(fieldVector), length, arrays), m_index->Slice(offset, length) };
}

DataFrame DataFrame::operator[](std::vector<std::string> const& columns) const
{
    std::vector<std::shared_ptr<arrow::ArrayData>> arrays;
    arrays.reserve(columns.size());

    arrow::FieldVector fieldVector;
    fieldVector.reserve(columns.size());

    auto schema = m_array->schema();

    for (auto const& column : columns)
    {
        auto idx = schema->GetFieldIndex(column);
        if (idx == -1)
        {
            std::stringstream ss;
            ss << "ValidColumns: \n";
            for(auto const& col : schema->field_names())
            {
                ss << col << " ";
            }
            ss << "Invalid column: " << column;
            throw std::runtime_error(ss.str());
        }
        arrays.emplace_back(m_array->column(idx)->data());
        fieldVector.emplace_back(schema->field(idx));
    }
    return { arrow::RecordBatch::Make(arrow::schema(fieldVector), m_array->num_rows(), arrays), m_index };
}

DataFrame DataFrame::slice(int offset) const
{
    return { m_array->Slice(offset), m_index->Slice(offset) };
}

Scalar DataFrame::sum() const
{
    auto status = arrow::compute::CallFunction("sum", { std::make_shared<arrow::ChunkedArray>(m_array->columns()) });
    if (status.ok())
    {
        return std::move(status->scalar()->shared_from_this());
    }
    else
    {
        throw std::runtime_error(status.status().ToString());
    }
}


Series DataFrame::forAxis(std::string const& functionName,
                          pd::AxisType axis) const
{
    arrow::ScalarVector result;

    pd::ArrayPtr newIndex;
    if (axis == AxisType::Columns)
    {
        auto columns = m_array->columns();
        result.resize(num_rows());
        tbb::parallel_for(
            0L,
            num_rows(),
            [&](int64_t i)
            {
                arrow::ScalarVector row;
                row.reserve(num_columns());

                for (const auto& column : columns)
                {
                    row.emplace_back(ReturnOrThrowOnFailure(column->GetScalar(i)));
                }
                result[i] = arrow::compute::CallFunction(functionName, { arrow::ScalarArray::Make(row) })->scalar();
            });
        newIndex = indexArray();
    }
    else
    {
        result.reserve(num_columns());

        for (const auto& column : m_array->columns())
        {
            result.emplace_back(arrow::compute::CallFunction(functionName, { column })->scalar());
        }

        newIndex = arrow::ArrayT<std::string>::Make(columnNames());
    }
    return pd::Series{ arrow::ScalarArray::Make(result), newIndex };
}


Series DataFrame::sum(pd::AxisType axis) const
{
    return forAxis("sum", axis);
}

Series DataFrame::count(pd::AxisType axis) const
{
    return forAxis("count", axis);
}

Series DataFrame::mean(pd::AxisType axis) const
{
    return forAxis("mean", axis);
}

DataFrame DataFrame::unary(std::string const& functionName) const
{
    if (m_array == nullptr)
    {
        return {};
    }

    std::vector<std::shared_ptr<arrow::ArrayData>> df_result;
    df_result.reserve(m_array->num_columns());
    for (const auto& col : m_array->columns())
    {
        auto result = arrow::compute::CallFunction(functionName, { col });
        if (result.ok())
        {
            df_result.emplace_back(result.MoveValueUnsafe().array());
        }
        else
        {
            throw std::runtime_error(result.status().ToString());
        }
    }
    return { m_array->schema(), m_array->num_rows(), df_result };
}


using namespace std::string_literals;
DataFrame DataFrame::describe(bool include_all, bool percentiles)
{

    if (m_array == nullptr)
    {
        return {};
    }

    const auto& valid_types = arrow::NumericTypes();
    if (std::ranges::any_of(
            m_array->schema()->fields(),
            [&valid_types](std::shared_ptr<arrow::Field> const& field)
            { return std::find(valid_types.begin(), valid_types.end(), field->type()) == valid_types.end(); }))
    {
        throw std::runtime_error("describe(): All Types must be numeric type");
    }

    std::vector<std::string> indexes;
    if (include_all)
    {
        indexes = this->columns();
    }
    else
    {
        for (auto const& field : this->array()->schema()->fields())
        {
            if (field->type()->id() != arrow::Type::NA)
            {
                indexes.push_back(field->name());
            }
        }
    }

    auto columns = std::vector{ "count"s, "mean"s, "std"s, "min"s, "nunique"s };

    std::vector<double> percentiles_list;
    if (percentiles)
    {
        columns.insert(columns.end(), { "25%"s, "50%"s, "75%"s });
        percentiles_list = { 0.25, 0.5, 0.75 };
    }

    columns.emplace_back("max");

    auto N = indexes.size();

    std::vector<long> counts(N);
    std::vector<double> mean(N);
    std::vector<double> std(N);
    arrow::ScalarVector min(N);
    std::vector<long> nunique(N);
    std::vector<arrow::ScalarVector> quantiles(percentiles_list.size(), arrow::ScalarVector{ N });
    arrow::ScalarVector max(N);

    auto indexesArray = arrow::ArrayT<std::string>::Make(indexes);
    for (int i = 0; i < indexes.size(); i++)
    {
        auto index = indexes[i];
        auto series = operator[](index);

        counts[i] = series.count();
        mean[i] = series.mean();
        std[i] = series.std();
        min[i] = series.min().value();
        nunique[i] = series.nunique();

        for (int j = 0; j < percentiles_list.size(); j++)
        {
            quantiles[j][i] = series.quantile(percentiles_list[j]).value();
        }

        max[i] = series.max().value();
    }

    arrow::ArrayVector data;
    arrow::FieldVector fields;

    auto common_type = min.back()->type; // todo

    fields.insert(
        fields.end(),
        {
            arrow::field("count", arrow::int64()),
            arrow::field("mean", arrow::float64()),
            arrow::field("std", arrow::float64()),
            arrow::field("min", common_type),
            arrow::field("nunique", arrow::int64()),
        });

    data.insert(
        data.end(),
        { arrow::ArrayT<long>::Make(counts),
          arrow::ArrayT<double>::Make(mean),
          arrow::ArrayT<double>::Make(std),
          arrow::ScalarArray::Make(min),
          arrow::ArrayT<long>::Make(nunique) });

    auto p = percentiles_list.begin();
    for (auto const& q : quantiles)
    {
        data.emplace_back(arrow::ScalarArray::Make(q));
        fields.emplace_back(arrow::field(std::to_string(int((*p++) * 100)).append("%"), common_type));
    }

    data.emplace_back(arrow::ScalarArray::Make(max));
    fields.emplace_back(arrow::field("max", common_type));

    return { arrow::schema(fields), static_cast<int64_t>(indexes.size()), data, indexesArray };
}

BINARY_OPERATOR_PARALLEL(-, subtract)

BINARY_OPERATOR_PARALLEL(+, add)

BINARY_OPERATOR_PARALLEL(/, divide)

BINARY_OPERATOR_PARALLEL(*, multiply)

BINARY_OPERATOR_PARALLEL(|, bit_wise_or)

BINARY_OPERATOR_PARALLEL(&, bit_wise_and)

BINARY_OPERATOR_PARALLEL(^, bit_wise_xor)

BINARY_OPERATOR_PARALLEL(<<, shift_left)

BINARY_OPERATOR_PARALLEL(>>, shift_right)

BINARY_OPERATOR_PARALLEL(>, greater)

BINARY_OPERATOR_PARALLEL(>=, greater_equal)

BINARY_OPERATOR_PARALLEL(<, less)

BINARY_OPERATOR_PARALLEL(<=, less_equal)

BINARY_OPERATOR_PARALLEL(==, equal)

BINARY_OPERATOR_PARALLEL(!=, not_equal)

BINARY_OPERATOR_PARALLEL(&&, and)

BINARY_OPERATOR_PARALLEL(||, or)

arrow::FieldVector DataFrame::dtypes() const
{
    return m_array->schema()->fields();
}

std::vector<std::string> DataFrame::columns() const
{
    return (m_array == nullptr) ? std::vector<std::string>{} : m_array->schema()->field_names();
}

DataFrame DataFrame::head(int length) const
{
    return { m_array->Slice(0, length), m_index->Slice(0, length) };
}

DataFrame DataFrame::tail(int length) const
{
    auto nRows = m_array->num_rows();
    auto startIndex = std::max<int64_t>(0l, nRows - length);
    return { m_array->Slice(startIndex, length),
             m_index->Slice(startIndex, length) };
}

DataFrame DataFrame::sort_index(bool ascending, bool ignore_index)
{
    Series index(m_index, nullptr, "", true);

    auto [sorted_values, sorted_indices] = index.sort(ascending);
    auto result = arrow::compute::CallFunction("take", { m_array, sorted_indices });
    if (result.ok())
    {
        return { result.MoveValueUnsafe().record_batch(), ignore_index ? nullptr : sorted_values };
    }
    throw std::runtime_error(result.status().ToString());
}

Series DataFrame::argsort(std::vector<std::string> const& fields, bool ascending) const
{
    arrow::compute::SortOptions opt;
    opt.sort_keys.reserve(fields.size());

    std::ranges::transform(
        fields,
        std::back_inserter(opt.sort_keys),
        [ascending](std::string const& name)
        {
            return arrow::compute::SortKey(
                name,
                ascending ? arrow::compute::SortOrder::Ascending : arrow::compute::SortOrder::Descending);
        });

    auto result = arrow::compute::CallFunction("sort_indices", { m_array }, &opt);
    if (result.ok())
    {
        return { result.MoveValueUnsafe().make_array(), false };
    }
    throw std::runtime_error(result.status().ToString());
}

DataFrame Series::value_counts() const
{
    auto result = arrow::compute::ValueCounts(m_array);
    if (result.ok())
    {
        return { result.MoveValueUnsafe(), std::vector<std::string>{ "values", "counts" } };
    }
    else
    {
        throw std::runtime_error(result.status().ToString());
    }
}

pd::DataFrame DataFrame::reindex(std::shared_ptr<arrow::Array> const& newIndex,
                                 const std::optional<Scalar>& fillValue) const noexcept
{
    auto N = m_array->num_columns();
    std::vector<pd::ArrayPtr> reindexedSeries(N);

    std::unordered_map<int64_t, int64_t> indexer;
    auto idx_int =
        pd::ReturnOrThrowOnFailure(arrow::compute::Cast(m_index, { arrow::int64() })).array_as<arrow::Int64Array>();

    for (int i = 0; i < m_index->length(); i++)
    {
        indexer[idx_int->Value(i)] = i;
    }

    std::ranges::transform(
        std::views::iota(0L, N),
        reindexedSeries.begin(),
        [&](std::int64_t i) { return Series(m_array->column(i), m_index).reindex(newIndex, indexer, fillValue).m_array; });
    return { m_array->schema(), newIndex->length(), reindexedSeries, newIndex };
}

pd::DataFrame DataFrame::reindexAsync(std::shared_ptr<arrow::Array> const& newIndex,
                                      const std::optional<Scalar>& fillValue) const noexcept
{
    auto N = m_array->num_columns();

    std::vector<pd::ArrayPtr> reindexedSeries(N);

    auto idx_int =
        pd::ReturnOrThrowOnFailure(arrow::compute::Cast(m_index, { arrow::int64() })).array_as<arrow::Int64Array>();

    std::unordered_map<int64_t, int64_t> indexer;
    for (int i = 0; i < m_index->length(); i++)
    {
        indexer[idx_int->Value(i)] = i;
    }

    tbb::parallel_for(
        0,
        N,
        [&](size_t i) { reindexedSeries[i] = Series(m_array->column(i), m_index).reindex(newIndex, indexer, fillValue).m_array; });

    return { m_array->schema(), newIndex->length(), reindexedSeries, newIndex };
}

DataFrame DataFrame::sort_values(const std::vector<std::string>& by, bool ascending, bool ignore_index)
{

    auto array = m_array;
    auto fields = m_array->schema()->field_names();

    for (auto const& field : by)
    {
        auto col = m_array->GetColumnByName(field);
        uint64_t i{};
        if (auto it = std::find(fields.begin(), fields.end(), field); it != fields.end())
        {
            i = std::distance(std::begin(fields), it);
        }
        else
        {
            throw std::runtime_error(field + " not in schema");
        }

        array = ReturnOrThrowOnFailure(
            array->SetColumn(i, arrow::field(field, col->type()), Series(col, nullptr, "", true).sort(ascending)[0]));
    }

    return array;
}

Series DataFrame::coalesce()
{
    std::vector<arrow::Datum> args(m_array->num_columns());
    std::copy(m_array->columns().begin(), m_array->columns().end(), args.begin());

    return ReturnSeriesOrThrowOnError(arrow::compute::CallFunction("coalesce", args));
}

DataFrame Series::mode(int n, bool skip_nulls) const
{
    auto modeWithCount =
        pd::ReturnOrThrowOnFailure(arrow::compute::Mode(m_array, arrow::compute::ModeOptions{ n, skip_nulls }));
    auto modeStruct = modeWithCount.array_as<arrow::StructArray>();

    return { modeStruct, { "mode", "count" } };
}

Series DataFrame::coalesce(std::vector<std::string> const& columns)
{
    std::vector<arrow::Datum> args(columns.size());
    std::transform(
        columns.begin(),
        columns.end(),
        args.begin(),
        [this](std::string const& key) { return m_array->GetColumnByName(key); });
    return ReturnSeriesOrThrowOnError(arrow::compute::CallFunction("coalesce", args));
}

GroupBy DataFrame::group_by(const std::string& key) const
{
    return { key, *this };
}

DataFrame DataFrame::drop_na() const
{
    auto N = num_columns();
    auto datum = ReturnOrThrowOnFailure(
        arrow::compute::DropNull(m_array->AddColumn(num_columns(), "___index___", m_index).MoveValueUnsafe()));
    auto new_rb = datum.record_batch();
    auto new_index = new_rb->column(N);
    new_rb = ReturnOrThrowOnFailure(new_rb->RemoveColumn(N));
    return { new_rb, new_index };
}

Resampler DataFrame::resample(
    std::string const& rule,
    bool closed_right,
    bool label_right,
    std::variant<ptime, TimeGrouperOrigin> const& origin,
    time_duration const& offset,
    std::string const& tz) const
{
    return pd::resample(*this, rule, closed_right, label_right, origin, offset, tz);
}

FOR_ALL_COLUMN(bfill)
FOR_ALL_COLUMN(ffill)
FOR_ALL_COLUMN(is_null)
FOR_ALL_COLUMN(is_valid)
FOR_ALL_COLUMN(is_finite)
FOR_ALL_COLUMN(is_infinite)

DataFrame DataFrame::transpose() const
{
    auto newIndex = arrow::ArrayT<std::string>::Make(m_array->schema()->field_names());
    arrow::FieldVector newFields(m_index->length());

    auto newColumnSize = newFields.size();
    auto newRowSize = newIndex->length();

    auto fields = m_array->schema()->fields();
    std::vector<std::shared_ptr<arrow::DataType>> data_types{ fields.size() };
    std::ranges::transform(
        fields,
        data_types.begin(),
        [](std::shared_ptr<arrow::Field> const& field) { return field->type(); });

    auto commonType = promoteTypes(data_types);

    auto builder = ReturnOrThrowOnFailure(arrow::MakeBuilder(commonType));

    arrow::ArrayVector arrays(newColumnSize);

    std::ranges::transform(
        std::views::iota(0UL, newColumnSize),
        arrays.begin(),
        [&](size_t i)
        {
            auto field = ReturnOrThrowOnFailure(m_index->GetScalar(i));
            arrow::ScalarVector scalars(newRowSize);

            for (size_t j = 0; j < newRowSize; j++)
            {
                scalars[j] =
                    ReturnOrThrowOnFailure(m_array->column(j)->GetScalar(i).MoveValueUnsafe()->CastTo(commonType));
            }

            newFields[i] = arrow::field(field->ToString(), commonType);

            ThrowOnFailure(builder->AppendScalars(scalars));
            auto data = ReturnOrThrowOnFailure(builder->Finish());
            return data;
        });

    return { arrow::schema(newFields), newRowSize, arrays, newIndex };
}

TablePtr DataFrame::toTable(std::optional<std::string> const& index_name) const
{
    auto name = index_name.value_or("index");
    auto recordBatchWithIndex = pd::ReturnOrThrowOnFailure(m_array->AddColumn(num_columns(), name, m_index));

    return pd::ReturnOrThrowOnFailure(arrow::Table::FromRecordBatches({ recordBatchWithIndex }));
}

arrow::Result<pd::DataFrame> GroupBy::apply_async(std::function<ScalarPtr(Series const&)> fn)
{
    std::shared_ptr<arrow::Schema> schema = df.m_array->schema();

    ::int64_t numGroups = groupSize();
    ::int64_t numColumns = schema->num_fields();

    arrow::ArrayDataVector resultForEachColumn(numGroups);
    auto columnNames = schema->field_names();

    tbb::parallel_for(
        0L,
        numColumns,
        [&](::int64_t columnIdx)
        {
            arrow::ScalarVector result(numGroups);
            std::string const& columnName = columnNames[columnIdx];
            std::ranges::transform(
                std::views::iota(0L, numGroups),
                result.begin(),
                [&](::int64_t groupIdx)
                {
                    ScalarPtr key = GetKeyByIndex(groupIdx);

                    ArrayPtr index = indexGroups[key];
                    int64_t numRows = index->length();

                    arrow::ArrayVector group = groups[key];
                    ArrayPtr columnInGroup = group[columnIdx];

                    auto seriesFromGroupArray = pd::Series(columnInGroup, index, columnName);
                    return fn(seriesFromGroupArray);
                });
            resultForEachColumn[columnIdx] = ReturnOrThrowOnFailure(buildData(result));
        });

    return pd::DataFrame(schema, numGroups, resultForEachColumn, uniqueKeys);
}

arrow::Result<pd::Series> GroupBy::apply_async(std::function<ScalarPtr(DataFrame const&)> fn)
{
    ::int64_t numGroups = groupSize();
    arrow::ScalarVector result(numGroups);
    std::shared_ptr<arrow::Schema> schema = df.m_array->schema();

    tbb::parallel_for(
        0L,
        numGroups,
        [&](::int64_t groupIdx)
        {
            ScalarPtr key = GetKeyByIndex(groupIdx);
            ArrayPtr index = indexGroups[key];
            arrow::ArrayVector group = groups[key];
            int64_t numRows = index->length();
            auto dataFrameGroup = pd::DataFrame(schema, numRows, group, index);
            result[groupIdx] = fn(dataFrameGroup);
        });

    ARROW_ASSIGN_OR_RAISE(auto finalArray, buildArray(result));
    return pd::Series(finalArray, uniqueKeys);
}

arrow::Result<pd::DataFrame> GroupBy::apply(std::function<ScalarPtr(Series const&)> fn)
{
    std::shared_ptr<arrow::Schema> schema = df.m_array->schema();

    ::int64_t numGroups = groupSize();
    ::int64_t numColumns = schema->num_fields();

    arrow::ArrayDataVector resultForEachColumn(numColumns);
    auto columnNames = schema->field_names();

    std::ranges::transform(
        std::views::iota(0L, numColumns),
        resultForEachColumn.begin(),
        [&](::int64_t columnIdx)
        {
            arrow::ScalarVector result(numGroups);
            std::string const& columnName = columnNames[columnIdx];
            std::ranges::transform(
                std::views::iota(0L, numGroups),
                result.begin(),
                [&](::int64_t groupIdx)
                {
                    ScalarPtr key = GetKeyByIndex(groupIdx);

                    ArrayPtr index = indexGroups[key];
                    int64_t numRows = index->length();

                    arrow::ArrayVector group = groups[key];
                    ArrayPtr columnInGroup = group[columnIdx];

                    auto seriesFromGroupArray = pd::Series(columnInGroup, index, columnName);
                    return fn(seriesFromGroupArray);
                });
            return ReturnOrThrowOnFailure(buildData(result));
        });

    return pd::DataFrame(schema, numGroups, resultForEachColumn);
}

arrow::Result<pd::Series> GroupBy::apply(std::function<ScalarPtr(DataFrame const&)> fn)
{
    int64_t numGroups = groupSize();
    arrow::ScalarVector result(numGroups);
    if (!df.m_array)
    {
        return arrow::Status::OK();
    }
    std::shared_ptr<arrow::Schema> schema = df.m_array->schema();

    std::ranges::transform(
        std::views::iota(0L, numGroups),
        result.begin(),
        [&](::int64_t i)
        {
            ScalarPtr key = GetKeyByIndex(i);
            ArrayPtr index = indexGroups[key];
            arrow::ArrayVector group = groups[key];
            int64_t numRows = index->length();
            auto dataFrameGroup = pd::DataFrame(schema, numRows, group, index);
            return fn(dataFrameGroup);
        });

    ARROW_ASSIGN_OR_RAISE(auto finalArray, buildArray(result));
    return pd::Series(finalArray, uniqueKeys);
}

GROUPBY_NUMERIC_AGG(mean, double)
GROUPBY_NUMERIC_AGG(approximate_median, double)
GROUPBY_NUMERIC_AGG(stddev, double)
GROUPBY_NUMERIC_AGG(tdigest, double)
GROUPBY_NUMERIC_AGG(variance, double)
GROUPBY_NUMERIC_AGG(all, bool)
GROUPBY_NUMERIC_AGG(any, bool)

GROUPBY_NUMERIC_AGG(count, int64_t)
GROUPBY_NUMERIC_AGG(count_distinct, int64_t)

GROUPBY_AGG(max)
GROUPBY_AGG(min)
GROUPBY_AGG(sum)
GROUPBY_AGG(product)


arrow::Status GroupBy::processEach(
    std::unique_ptr<arrow::compute::Grouper> const& grouper,
    std::shared_ptr<arrow::ListArray> const& groupings,
    std::shared_ptr<arrow::Array> const& column)
{
    using namespace arrow;
    using namespace arrow::compute;

    ARROW_ASSIGN_OR_RAISE(auto grouped_argument, Grouper::ApplyGroupings(*groupings, *column));

    for (int64_t i_group = 0; i_group < grouper->num_groups(); ++i_group)
    {
        ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Scalar> keyScalar, uniqueKeys->GetScalar(i_group));

        groups[keyScalar].emplace_back(grouped_argument->value_slice(i_group));
    }
    return arrow::Status::OK();
}

arrow::Status GroupBy::processIndex(
    std::unique_ptr<arrow::compute::Grouper> const& grouper,
    std::shared_ptr<arrow::ListArray> const& groupings)
{
    using namespace arrow;
    using namespace arrow::compute;

    ARROW_ASSIGN_OR_RAISE(auto grouped_argument, Grouper::ApplyGroupings(*groupings, *df.indexArray()));

    for (int64_t i_group = 0; i_group < grouper->num_groups(); ++i_group)
    {
        ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Scalar> keyScalar, uniqueKeys->GetScalar(i_group));
        indexGroups[keyScalar] = grouped_argument->value_slice(i_group);
    }
    return arrow::Status::OK();
}

arrow::Status GroupBy::makeGroups(std::string const& keyInStringFormat)
{
    using namespace arrow;
    using namespace arrow::compute;
    if (!df.m_array)
    {
        return arrow::Status::OK();
    }

    auto schema = df.array()->schema();
    auto key_array = keyInStringFormat == "__resampler_idx__" ? df.indexArray() : df[keyInStringFormat].array();
    ARROW_ASSIGN_OR_RAISE(auto key_batch, ExecBatch::Make(std::vector<Datum>{ key_array }));

    ARROW_ASSIGN_OR_RAISE(auto grouper, Grouper::Make(key_batch.GetTypes()));

    ARROW_ASSIGN_OR_RAISE(Datum id_batch, grouper->Consume(ExecSpan(key_batch)));

    ARROW_ASSIGN_OR_RAISE(
        auto groupings,
        Grouper::MakeGroupings(*id_batch.array_as<UInt32Array>(), grouper->num_groups()));

    ARROW_ASSIGN_OR_RAISE(auto uniques, grouper->GetUniques());
    uniqueKeys = uniques.values[0].make_array();

    RETURN_NOT_OK(processIndex(grouper, groupings));

    for (auto const& col : df.m_array->columns())
    {
        RETURN_NOT_OK(processEach(grouper, groupings, col));
    }

    return arrow::Status::OK();
}

arrow::Result<pd::DataFrame> GroupBy::min_max(std::vector<std::string> const& args)
{
    auto schema = df.m_array->schema();
    auto N = groups.size();

    arrow::FieldVector fv;
    for (auto const& arg : args)
    {
        auto type = schema->GetFieldByName(arg)->type();
        fv.push_back(arrow::field(arg + "_min", type));
        fv.push_back(arrow::field(arg + "_max", type));
    }

    long keysLength = uniqueKeys->length();
    arrow::ArrayDataVector array(fv.size());

    for (size_t i = 0; i < args.size(); i++)
    {
        const auto& arg = args[i];
        int index = schema->GetFieldIndex(arg);

        arrow::ScalarVector min(keysLength), max(keysLength);
        tbb::parallel_for(
            0L,
            keysLength,
            [&](size_t j)
            {
                auto key = uniqueKeys->GetScalar(long(j)).MoveValueUnsafe();
                auto& group = groups.at(key);

                auto d =
                    ReturnOrThrowOnFailure(arrow::compute::MinMax(group[index])).scalar_as<arrow::StructScalar>().value;
                min[j] = d[0];
                max[j] = d[1];
            });

        std::shared_ptr<arrow::ArrayBuilder> minBuilder, maxBuilder;
        if (not min.empty())
        {
            minBuilder = ReturnOrThrowOnFailure(arrow::MakeBuilder(min.back()->type));
            maxBuilder = ReturnOrThrowOnFailure(arrow::MakeBuilder(max.back()->type));

            RETURN_NOT_OK(minBuilder->AppendScalars(min));
            RETURN_NOT_OK(maxBuilder->AppendScalars(max));
        }

        std::shared_ptr<arrow::ArrayData> min_data, max_data;
        RETURN_NOT_OK(minBuilder->FinishInternal(&min_data));
        RETURN_NOT_OK(maxBuilder->FinishInternal(&max_data));

        array[i * 2] = min_data;
        array[i * 2 + 1] = max_data;
    }

    return pd::DataFrame(arrow::schema(fv), long(N), array);
}

arrow::Result<pd::DataFrame> GroupBy::min_max(std::string const& arg)
{
    auto schema = df.m_array->schema();
    auto N = groups.size();

    auto fv = schema->GetFieldByName(arg);

    long L = uniqueKeys->length();
    int index = schema->GetFieldIndex(arg);

    arrow::ScalarVector min(L), max(L);
    tbb::parallel_for(
        0l,
        L,
        [&](size_t j)

        {
            auto key = uniqueKeys->GetScalar(long(j)).MoveValueUnsafe();
            auto& group = groups.at(key);

            auto d =
                ReturnOrThrowOnFailure(arrow::compute::MinMax(group[index])).scalar_as<arrow::StructScalar>().value;
            min[j] = d[0];
            max[j] = d[1];
        });

    std::shared_ptr<arrow::DataType> dtype;
    std::shared_ptr<arrow::ArrayBuilder> minBuilder, maxBuilder;
    if (not min.empty())
    {
        dtype = min.back()->type;
        minBuilder = ReturnOrThrowOnFailure(arrow::MakeBuilder(dtype));
        maxBuilder = ReturnOrThrowOnFailure(arrow::MakeBuilder(dtype));

        RETURN_NOT_OK(minBuilder->AppendScalars(min));
        RETURN_NOT_OK(maxBuilder->AppendScalars(max));
    }

    std::shared_ptr<arrow::ArrayData> min_data, max_data;
    RETURN_NOT_OK(minBuilder->FinishInternal(&min_data));
    RETURN_NOT_OK(maxBuilder->FinishInternal(&max_data));

    return pd::DataFrame{ arrow::schema(arrow::FieldVector{ arrow::field("min", dtype), arrow::field("max", dtype) }),
                          L,
                          arrow::ArrayDataVector{ min_data, max_data } };
}

arrow::Result<pd::DataFrame> GroupBy::first(std::vector<std::string> const& args)
{
    auto schema = df.m_array->schema();
    auto N = groups.size();

    auto fv = fieldVectors(args, schema);
    arrow::ArrayDataVector arr(args.size());

    tbb::parallel_for(
        tbb::blocked_range<size_t>(0, args.size()),
        [&](const tbb::blocked_range<size_t>& r)
        {
            for (size_t i = r.begin(); i != r.end(); ++i)
            {
                const auto& arg = args[i];
                long L = uniqueKeys->length();
                int index = schema->GetFieldIndex(arg);

                arrow::ScalarVector result(L);
                tbb::parallel_for(
                    0L,
                    L,
                    [&](size_t j)
                    {
                        auto key = uniqueKeys->GetScalar(long(j)).MoveValueUnsafe();
                        auto& group = groups.at(key);
                        result[j] = ReturnOrThrowOnFailure(group[index]->GetScalar(0));
                    });

                arr[i] = ReturnOrThrowOnFailure(buildData(result));
            }
        });

    return pd::DataFrame(arrow::schema(fv), long(N), arr, uniqueKeys);
}

arrow::Result<pd::Series> GroupBy::first(std::string const& arg)
{
    auto schema = df.m_array->schema();
    auto N = groups.size();

    auto fv = schema->GetFieldByName(arg);

    long L = uniqueKeys->length();
    int index = schema->GetFieldIndex(arg);

    arrow::ScalarVector result(L);
    tbb::blocked_range<size_t> r(0, L);
    for (size_t j = r.begin(); j != r.end(); ++j)
    {
        auto key = uniqueKeys->GetScalar(long(j)).MoveValueUnsafe();
        auto& group = groups.at(key);

        ARROW_ASSIGN_OR_RAISE(result[j], group[index]->GetScalar(0));
    }

    ARROW_ASSIGN_OR_RAISE(auto data, buildArray(result));
    return pd::Series(data, nullptr);
}

arrow::Result<pd::DataFrame> GroupBy::last(std::vector<std::string> const& args)
{
    auto schema = df.m_array->schema();
    auto N = groups.size();

    auto fv = fieldVectors(args, schema);
    arrow::ArrayDataVector arr(args.size());

    tbb::parallel_for(
        0ul,
        args.size(),
        [&](size_t i)
        {
            const auto& arg = args[i];
            long L = uniqueKeys->length();
            int index = schema->GetFieldIndex(arg);

            arrow::ScalarVector result(L);
            tbb::parallel_for(
                0L,
                L,
                [&](size_t j)
                {
                    auto key = uniqueKeys->GetScalar(long(j)).MoveValueUnsafe();
                    auto& group = groups.at(key);
                    auto groupLength = group[index]->length();
                    auto lastIndex = groupLength - 1;
                    result[j] = ReturnOrThrowOnFailure(group[index]->GetScalar(lastIndex));
                });

            arr[i] = ReturnOrThrowOnFailure(buildData(result));
        });

    return pd::DataFrame(arrow::schema(fv), long(N), arr);
}

arrow::Result<pd::Series> GroupBy::last(std::string const& arg)
{
    auto schema = df.m_array->schema();
    auto N = groups.size();

    auto fv = schema->GetFieldByName(arg);

    long L = uniqueKeys->length();
    int index = schema->GetFieldIndex(arg);

    arrow::ScalarVector result(L);
    std::ranges::transform(
        std::views::iota(0, L),
        result.begin(),
        [&](int j)
        {
            auto key = uniqueKeys->GetScalar(long(j)).MoveValueUnsafe();
            auto& group = groups.at(key);
            auto groupLength = group[index]->length();
            auto lastIndex = groupLength - 1;
            return ReturnOrThrowOnFailure(group[index]->GetScalar(lastIndex));
        });

    ARROW_ASSIGN_OR_RAISE(auto data, buildArray(result));
    return pd::Series(data, uniqueKeys);
}

arrow::Result<pd::DataFrame> GroupBy::mode(std::vector<std::string> const& args)
{
    auto schema = df.m_array->schema();
    auto N = groups.size();

    auto fv = fieldVectors(args, schema);
    arrow::ArrayDataVector arr(args.size());

    tbb::parallel_for(
        tbb::blocked_range<size_t>(0, args.size()),
        [&](const tbb::blocked_range<size_t>& r)
        {
            for (size_t i = r.begin(); i != r.end(); ++i)
            {
                const auto& arg = args[i];
                long L = uniqueKeys->length();
                int index = schema->GetFieldIndex(arg);

                arrow::ScalarVector result(L);
                tbb::parallel_for(
                    tbb::blocked_range<size_t>(0, uniqueKeys->length()),
                    [&](const tbb::blocked_range<size_t>& r)
                    {
                        for (size_t j = r.begin(); j != r.end(); ++j)
                        {
                            auto key = uniqueKeys->GetScalar(long(j)).MoveValueUnsafe();
                            auto& group = groups.at(key);

                            arrow::Datum d = ReturnOrThrowOnFailure(arrow::compute::Mode(group[index]));
                            result[j] = d.scalar();
                        }
                    });
                arr[i] = pd::ReturnOrThrowOnFailure(buildData(result));
            }
        });

    return pd::DataFrame(arrow::schema(fv), long(N), arr);
}

arrow::Result<pd::Series> GroupBy::mode(std::string const& arg)
{
    auto schema = df.m_array->schema();
    auto N = groups.size();

    auto fv = schema->GetFieldByName(arg);

    long L = uniqueKeys->length();
    int index = schema->GetFieldIndex(arg);

    arrow::ScalarVector result(L);
    tbb::parallel_for(
        tbb::blocked_range<size_t>(0, L),
        [&](const tbb::blocked_range<size_t>& r)
        {
            for (size_t j = r.begin(); j != r.end(); ++j)
            {
                auto key = uniqueKeys->GetScalar(long(j)).MoveValueUnsafe();
                auto& group = groups.at(key);

                arrow::Datum d = ReturnOrThrowOnFailure(arrow::compute::Mode(group[index]));
                result[j] = d.scalar();
            }
        });

    ARROW_ASSIGN_OR_RAISE(auto data, buildArray(result));
    return pd::Series(data, uniqueKeys);
}

arrow::Result<pd::DataFrame> GroupBy::quantile(std::vector<std::string> const& args, std::vector<double> const& q)
{
    auto schema = df.m_array->schema();
    auto N = groups.size();

    auto fv = fieldVectors(args, schema);
    arrow::ArrayDataVector arr(args.size());

    auto options = convertToArrowFunctionOptions<arrow::compute::QuantileOptions>(q);
    tbb::parallel_for(
        tbb::blocked_range<size_t>(0, args.size()),
        [&](const tbb::blocked_range<size_t>& r)
        {
            for (size_t i = r.begin(); i != r.end(); ++i)
            {
                const auto& arg = args[i];
                long L = uniqueKeys->length();
                int index = schema->GetFieldIndex(arg);

                arrow::ScalarVector result(L);
                tbb::parallel_for(
                    tbb::blocked_range<size_t>(0, uniqueKeys->length()),
                    [&](const tbb::blocked_range<size_t>& r)
                    {
                        for (size_t j = r.begin(); j != r.end(); ++j)
                        {
                            auto key = uniqueKeys->GetScalar(long(j)).MoveValueUnsafe();
                            auto& group = groups.at(key);

                            arrow::Datum d = ReturnOrThrowOnFailure(arrow::compute::Quantile(group[index], options[i]));
                            result[j] = d.scalar();
                        }
                    });
                arr[i] = ReturnOrThrowOnFailure(buildData(result));
            }
        });

    return pd::DataFrame(arrow::schema(fv), long(N), arr);
}

arrow::Result<pd::Series> GroupBy::quantile(std::string const& arg, double q)
{
    auto schema = df.m_array->schema();
    auto N = groups.size();

    auto fv = schema->GetFieldByName(arg);

    long L = uniqueKeys->length();
    int index = schema->GetFieldIndex(arg);
    if (index == -1)
    {
        throw std::runtime_error("Invalid column: " + arg);
    }

    arrow::compute::QuantileOptions option(q);

    arrow::ScalarVector result(L);
    tbb::parallel_for(
        tbb::blocked_range<size_t>(0, L),
        [&](const tbb::blocked_range<size_t>& r)
        {
            for (size_t j = r.begin(); j != r.end(); ++j)
            {
                auto key = uniqueKeys->GetScalar(long(j)).MoveValueUnsafe();
                auto& group = groups.at(key);

                arrow::Datum d = ReturnOrThrowOnFailure(arrow::compute::Quantile(group[index], option));
                result[j] = d.scalar();
            }
        });

    ARROW_ASSIGN_OR_RAISE(auto data, buildArray(result));
    return pd::Series(data, uniqueKeys);
}
} // namespace pd
