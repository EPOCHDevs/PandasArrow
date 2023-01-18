//
// Created by dewe on 12/27/22.
//

#include <arrow/api.h>
#include <arrow/io/api.h>
#include "filesystem"
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <iostream>
#include <arrow/compute/exec.h>
#include "arrow/array.h"
#include "arrow/array/concatenate.h"
#include "arrow/chunked_array.h"
#include "arrow/compute/api_aggregate.h"
#include "arrow/compute/api_vector.h"
#include "arrow/compute/cast.h"
#include "arrow/compute/exec/aggregate.h"
#include "arrow/compute/registry.h"
#include "arrow/table.h"
#include "arrow/type.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/int_util_overflow.h"
#include "arrow/util/key_value_metadata.h"
#include "arrow/util/vector.h"
#include "group_by.h"
#include "datetimelike.h"
#include "dataframe.h"


#define BINARY_OPERATOR(sign, func)                                                                 \
DataFrame DataFrame::operator sign(Series const &s) const                      \
{                                                                                                   \
    std::vector<std::shared_ptr<arrow::ArrayData>> df_result;                                       \
    df_result.reserve(m_array->num_columns());                                                      \
    for (const auto &col: m_array->columns()) {                                                     \
        auto result = arrow::compute::CallFunction( #func, {col, s.m_array});                       \
        if (result.ok()) {                                                                          \
            df_result.emplace_back(result.MoveValueUnsafe().array());                               \
        } else {                                                                                    \
            throw std::runtime_error(result.status().ToString());                                   \
        }                                                                                           \
    }                                                                                               \
    return {m_array->schema(), m_array->num_rows(), df_result};                                     \
}                                                                                                   \
                                                                                                    \
DataFrame DataFrame::operator sign (Scalar const &s) const                                          \
{                                                                                                   \
    std::vector<std::shared_ptr<arrow::ArrayData>> df_result;                                       \
    df_result.reserve(m_array->num_columns());                                                      \
    for (const auto &col: m_array->columns()) {                                                     \
        auto result = arrow::compute::CallFunction(# func, {col, s.value()});                       \
        if (result.ok()) {                                                                          \
            df_result.emplace_back(result.MoveValueUnsafe().array());                               \
        } else {                                                                                    \
            throw std::runtime_error(result.status().ToString());                                   \
        }                                                                                           \
    }                                                                                               \
    return {m_array->schema(), m_array->num_rows(), df_result};                                     \
}                                                                                                   \
                                                                                                    \
DataFrame DataFrame::operator sign ( DataFrame const& s)const                                       \
{                                                                                                   \
    if(s.m_array->num_columns() == m_array->num_columns()){                                         \
    std::vector<std::shared_ptr<arrow::ArrayData>> df_result;                                       \
    df_result.resize(m_array->num_columns());                                                       \
    auto columns = m_array->columns();                                                              \
    std::transform(columns.begin(),                                                                 \
        columns.end(),                                                                              \
        s.m_array->columns().begin(),                                                               \
        df_result.begin(),                                                                          \
        [](std::shared_ptr<arrow::Array> const &a,                                                  \
        std::shared_ptr<arrow::Array> const &b) -> std::shared_ptr<arrow::ArrayData> {              \
        auto result = arrow::compute::CallFunction(#func, {a, b});                                  \
        if (result.ok())                                                                            \
        {                                                                                            \
            return result.MoveValueUnsafe().array();                                                 \
        } else {                                                                                    \
            throw std::runtime_error(result.status().ToString());                                    \
        }});                                                                                         \
        return {m_array->schema(), m_array->num_rows(), df_result};                                 \
        }                                                                                           \
        else                                                                                        \
        {                                                                                           \
        throw std::runtime_error("Broadcasting not allowed for DataFrame");                         \
        }                                                                                           \
}                                                                                                   \
DataFrame operator sign(Series const &s, DataFrame const& df) {                                     \
    std::vector<std::shared_ptr<arrow::ArrayData>> df_result;                                       \
    df_result.reserve(df.m_array->num_columns());                                                   \
    for (const auto &col: df.m_array->columns()) {                                                  \
        auto result = arrow::compute::CallFunction(#func, {s.array(), col});                        \
        if (result.ok()) {                                                                          \
            df_result.emplace_back(result.MoveValueUnsafe().array());                               \
        } else {                                                                                    \
            throw std::runtime_error(result.status().ToString());                                   \
        }                                                                                           \
    }                                                                                               \
    return {df.m_array->schema(), df.m_array->num_rows(), df_result};                               \
}                                                                                                   \
DataFrame operator sign(Scalar const &s, DataFrame const& df)                                       \
{                                                                                                   \
    std::vector<std::shared_ptr<arrow::ArrayData>> df_result;                                       \
    df_result.reserve(df.m_array->num_columns());                                                   \
    for (const auto &col: df.m_array->columns()) {                                                  \
        auto result = arrow::compute::CallFunction(#func, {s.value(), col});                        \
        if (result.ok()) {                                                                          \
            df_result.emplace_back(result.MoveValueUnsafe().array());                               \
        } else {                                                                                    \
            throw std::runtime_error(result.status().ToString());                                   \
        }                                                                                           \
    }                                                                                               \
    return {df.m_array->schema(), df.m_array->num_rows(), df_result};                               \
}                                                                                                   \

namespace pd {

DataFrame::DataFrame(
    const std::shared_ptr<arrow::RecordBatch>& table,
    const std::shared_ptr<arrow::Array>& _index)
    : NDArray(table, _index)
{
    if (not _index)
    {
        if(table)
        {
            m_index = uint_range(table->num_rows());
        }
    }
}

bool DataFrame::contains(std::string const& column)
{
    return m_array->schema()->GetFieldIndex(column) != -1;
}

DataFrame::DataFrame(
    std::shared_ptr<arrow::StructArray> const& arr,
    std::vector<std::string> const& columns)
    : NDArray<DataFrame>(nullptr)
{
    if (columns.empty())
    {
        throw std::runtime_error("Creating empty DataFrame is not permitted\n");
    }

    arrow::FieldVector fields;
    arrow::ArrayVector arrayVector;

    for (auto const& column : columns)
    {
        auto array = arr->GetFieldByName(column);
        fields.emplace_back(arrow::field(column, arr->type()));
        arrayVector.emplace_back(array);
    }

    int num_rows = arrayVector.back()->length();
    m_index = uint_range(num_rows);
    m_array =
        arrow::RecordBatch::Make(arrow::schema(fields), num_rows, arrayVector);
}

DataFrame& DataFrame::rename(
    std::unordered_map<std::string, std::string> const& columns)
{
    auto schemas = m_array->schema();

    for (auto const& [old, new_] : columns)
    {
        auto index = schemas->GetFieldIndex(old);
        if(index != -1)
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

void DataFrame::add_prefix(std::string const& prefix)
{
    std::unordered_map<std::string, std::string> new_columns;
    for (auto const& name : m_array->schema()->field_names())
    {
        new_columns[name] = prefix + name;
    }
    rename(new_columns);
}

void DataFrame::add_suffix(std::string const& suffix)
{
    std::unordered_map<std::string, std::string> new_columns;
    for (auto const& name : m_array->schema()->field_names())
    {
        new_columns[name] = name + suffix;
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
    return { pd::ValidateAndReturn(m_array->RemoveColumn(column_idx)),
             new_index };
}

DataFrame DataFrame::readParquet(std::filesystem::path const& path)
{

    auto&& infileStatus = arrow::io::ReadableFile::Open(path);
    if (infileStatus.ok())
    {
        auto infile = std::move(infileStatus).ValueUnsafe();

        std::unique_ptr<parquet::arrow::FileReader> reader;
        // Open up the file with the IPC features of the library, gives us a reader object.
        PARQUET_THROW_NOT_OK(parquet::arrow::OpenFile(
            infile,
            arrow::default_memory_pool(),
            &reader));

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
                throw std::runtime_error(
                    "Cannot Initialize DataFrame with empty parquet table");
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

std::ostream& operator<<(std::ostream& os, DataFrame const& df)
{
    tabulate::Table table;

    int64_t nRows = df.m_array->num_rows();
    int64_t nCols = df.m_array->num_columns();

    const auto halfRow = int(PD_MAX_ROW_TO_PRINT / 2);
    const auto halfCol = int(PD_MAX_COL_TO_PRINT / 2);

    std::vector<std::variant<std::string, tabulate::Table>> dot{ halfRow, "" };
    std::vector<std::variant<std::string, tabulate::Table>> cells;
    cells.emplace_back("index");
    for (int j = 0; j < nCols; j++)
    {
        if (nCols <= PD_MAX_COL_TO_PRINT or
            (nCols > PD_MAX_COL_TO_PRINT and
             (j < halfCol or j > nCols - halfCol)))
        {
            cells.emplace_back(df.m_array->schema()->field(j)->name());
        }
    }
    table.add_row(cells);

    bool added_sep = false;
    for (int64_t i = 0; i < nRows; i++)
    {
        if (nRows <= PD_MAX_ROW_TO_PRINT or
            (nRows > PD_MAX_ROW_TO_PRINT and
             (i < halfRow or i > (nRows - halfRow))))
        {
            cells.clear();
            cells.emplace_back(
                df.m_index->GetScalar(i).MoveValueUnsafe()->ToString());
            for (int j = 0; j < nCols; j++)
            {
                if (nCols <= PD_MAX_COL_TO_PRINT or
                    (nCols > PD_MAX_COL_TO_PRINT and
                     (j < halfCol or j > nCols - halfCol)))
                {
                    cells.emplace_back(df.m_array->column(j)
                                           ->GetScalar(i)
                                           .MoveValueUnsafe()
                                           ->ToString());
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
        return { ptr, m_index, column };
    throw std::runtime_error(column + " is not a valid column");
}

std::unordered_map<std::string, pd::Scalar>
    DataFrame::operator[](int64_t row) const
{
    std::unordered_map<std::string, pd::Scalar> result;
    auto columns = columnNames();
    std::transform(
        columns.begin(),
        columns.end(),
        std::inserter(result, std::end(result)),
        [this, row](std::string const& column)
        {
            return std::pair{
                column,
                pd::ValidateAndReturn(
                    this->m_array->GetColumnByName(column)->GetScalar(row))
            };
        });
    return result;
}

DataFrame DateTimeLike::iso_calendar() const
{
    auto result = pd::ValidateAndReturn(arrow::compute::ISOCalendar(m_array));
    return {
        result.array_as<arrow::StructArray>(),
        std::vector<std::string>{ "iso_year", "iso_week", "iso_day_of_week" }
    };
}

DataFrame DateTimeLike::year_month_day() const
{
    auto result = pd::ValidateAndReturn(arrow::compute::ISOCalendar(m_array));
    return {
        result.array_as<arrow::StructArray>(),
        std::vector<std::string>{ "year", "month", "day" }
    };
}

Scalar DataFrame::at(int64_t row, int64_t col) const
{

    if (col < num_columns())
    {
        auto ptr = m_array->column(col);
        auto result = ptr->GetScalar(row);
        if(result.ok())
        {
            return result.MoveValueUnsafe();
        }
        throw std::runtime_error(std::to_string(row) +
                                 " is an invalid row index for column " +
                                 columnNames().at(col));
    }

    throw std::runtime_error(std::to_string(col) + " is not a valid column index");
}

std::vector<std::string> DataFrame::columnNames() const
{
    return m_array->schema()->field_names();
}

DataFrame DataFrame::slice(
    DateTimeSlice slicer,
    const std::vector<std::string>& columns) const
{
    if (m_index->type_id() == arrow::Type::TIMESTAMP)
    {
        int64_t start = 0, end = m_index->length() - 1;

        if (slicer.start)
        {
            start = ValidateHelperScalar<int64_t>(arrow::compute::Index(
                m_index,
                arrow::compute::IndexOptions{
                    fromDateTime(slicer.start.value()) }));
        }
        if (slicer.end)
        {
            end = ValidateHelperScalar<int64_t>(arrow::compute::Index(
                m_index,
                arrow::compute::IndexOptions{
                    fromDateTime(slicer.end.value()) }));
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

DataFrame DataFrame::slice(Slice slice, std::vector<std::string> const& columns)
    const
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
        arrays.emplace_back(
            m_array->column(idx)->data()->Slice(slice.start, length));
        fieldVector.emplace_back(schema->field(idx));
    }
    return {
        arrow::RecordBatch::Make(arrow::schema(fieldVector), length, arrays),
        m_index->Slice(slice.start, length)
    };
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
    return { arrow::RecordBatch::Make(schema, length, arrays),
             m_index->Slice(slice.start, length) };
}

DataFrame DataFrame::DataFrame::slice(DateTimeSlice slicer) const
{
    if (m_index->type_id() == arrow::Type::TIMESTAMP)
    {
        int64_t start = 0, end = m_index->length() - 1;

        if (slicer.start)
        {
            start = ValidateHelperScalar<int64_t>(arrow::compute::Index(
                m_index,
                arrow::compute::IndexOptions{
                    fromDateTime(slicer.start.value()) }));
        }
        if (slicer.end)
        {
            end = ValidateHelperScalar<int64_t>(arrow::compute::Index(
                m_index,
                arrow::compute::IndexOptions{
                    fromDateTime(slicer.end.value()) }));
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

DataFrame DataFrame::slice(int offset, std::vector<std::string> const& columns)
    const
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
        arrays.emplace_back(
            m_array->column(idx)->data()->Slice(offset, length));
        fieldVector.emplace_back(schema->field(idx));
    }
    return {
        arrow::RecordBatch::Make(arrow::schema(fieldVector), length, arrays),
        m_index->Slice(offset, length)
    };
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
        arrays.emplace_back(m_array->column(idx)->data());
        fieldVector.emplace_back(schema->field(idx));
    }
    return { arrow::RecordBatch::Make(
                 arrow::schema(fieldVector),
                 m_array->num_rows(),
                 arrays),
             m_index };
}

DataFrame DataFrame::slice(int offset) const
{
    return { m_array->Slice(offset), m_index->Slice(offset) };
}

Scalar DataFrame::sum() const
{
    auto status = arrow::compute::CallFunction(
        "sum",
        { std::make_shared<arrow::ChunkedArray>(m_array->columns()) });
    if (status.ok())
    {
        return std::move(status->scalar()->shared_from_this());
    }
    else
    {
        throw std::runtime_error(status.status().ToString());
    }
}

DataFrame DataFrame::operator-() const
{
    std::vector<std::shared_ptr<arrow::ArrayData>> df_result;
    df_result.reserve(m_array->num_columns());
    for (const auto& col : m_array->columns())
    {
        auto result = arrow::compute::CallFunction("Negate", { col });
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

DataFrame DataFrame::describe()
{
    std::unordered_map<std::string, std::vector<double>> stat;

    std::vector<std::string> index;
    int i = 0;
    for (auto const& col : m_array->columns())
    {
        Series s(col, false);
        index.emplace_back(m_array->schema()->field(i++)->name());
        stat["count"].emplace_back(s.count());
        stat["mean"].emplace_back(s.mean());
        stat["std"].emplace_back(s.std());
        stat["min"].emplace_back(
            Scalar(s.min().scalar->CastTo(DoubleTypePtr).MoveValueUnsafe())
                .as<double>());
        //            stat["25%"].emplace_back( Scalar(s.quantile(0.25).scalar->CastTo(DoubleTypePtr).MoveValueUnsafe()).as<double>());
        //            stat["50%"].emplace_back( Scalar(s.quantile().scalar->CastTo(DoubleTypePtr).MoveValueUnsafe()).as<double>() ); stat["75%"].emplace_back( Scalar(s.quantile(0.75).scalar->CastTo(DoubleTypePtr).MoveValueUnsafe()).as<double>() );
        stat["max"].emplace_back(s.max().as<double>());
    }

    arrow::StringBuilder builder;
    auto status = builder.AppendValues(index);
    if (status.ok())
    {
        return { stat, builder.Finish().MoveValueUnsafe() };
    }
    throwOnNotOkStatus(status);
    return { nullptr };
}

BINARY_OPERATOR(-, subtract)

BINARY_OPERATOR(+, add)

BINARY_OPERATOR(/, divide)

BINARY_OPERATOR(*, multiply)

BINARY_OPERATOR(|, bit_wise_or)

BINARY_OPERATOR(&, bit_wise_and)

BINARY_OPERATOR(^, bit_wise_xor)

BINARY_OPERATOR(<<, shift_left)

BINARY_OPERATOR(>>, shift_right)

BINARY_OPERATOR(>, greater)

BINARY_OPERATOR(>=, greater_equal)

BINARY_OPERATOR(<, less)

BINARY_OPERATOR(<=, less_equal)

BINARY_OPERATOR(==, equal)

BINARY_OPERATOR(!=, not_equal)

BINARY_OPERATOR(&&, and)

BINARY_OPERATOR(||, or)

arrow::FieldVector DataFrame::dtypes() const
{
    return m_array->schema()->fields();
}

std::vector<std::string> DataFrame::columns() const
{
    return m_array->schema()->field_names();
}

DataFrame DataFrame::head(int length) const
{
    return m_array->Slice(0, length);
}

DataFrame DataFrame::tail(int length) const
{
    return m_array->Slice(m_array->num_rows() - length, length);
}

DataFrame DataFrame::sort_index(bool ascending, bool ignore_index)
{
    Series index(m_index, nullptr, "", true);

    auto [sorted_values, sorted_indices] = index.sort(ascending);
    auto result =
        arrow::compute::CallFunction("take", { m_array, sorted_indices });
    if (result.ok())
    {
        return { result.MoveValueUnsafe().record_batch(),
                 ignore_index ? nullptr : sorted_values };
    }
    throw std::runtime_error(result.status().ToString());
}

Series DataFrame::argsort(
    std::vector<std::string> const& fields,
    bool ascending) const
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
                ascending ? arrow::compute::SortOrder::Ascending :
                            arrow::compute::SortOrder::Descending);
        });

    auto result =
        arrow::compute::CallFunction("sort_indices", { m_array }, &opt);
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
        return { result.MoveValueUnsafe(),
                 std::vector<std::string>{ "values", "counts" } };
    }
    else
    {
        throw std::runtime_error(result.status().ToString());
    }
}

DataFrame DataFrame::sort_values(
    const std::vector<std::string>& by,
    bool ascending,
    bool ignore_index)
{

    auto array = m_array;
    auto fields = m_array->schema()->field_names();

    for (auto const& field : by)
    {
        auto col = m_array->GetColumnByName(field);
        uint64_t i{};
        if (auto it = std::find(fields.begin(), fields.end(), field);
            it != fields.end())
        {
            i = std::distance(std::begin(fields), it);
        }
        else
        {
            throw std::runtime_error(field + " not in schema");
        }

        auto result = array->SetColumn(
            i,
            arrow::field(field, col->type()),
            Series(col, nullptr, "", true).sort(ascending)[0]);
        if (result.ok())
        {
            array = result.MoveValueUnsafe();
        }
        else
        {
            throwOnNotOkStatus(result.status());
        }
    }

    return array;
}

Series DataFrame::coalesce()
{
    std::vector<arrow::Datum> args(m_array->num_columns());
    std::ranges::copy(
        m_array->columns().begin(),
        m_array->columns().end(),
        args.begin());
    auto result = arrow::compute::CallFunction("coalesce", args);
    if (result.ok())
    {
        return { result.MoveValueUnsafe().make_array(), false };
    }
    throwOnNotOkStatus(result.status());
    return { nullptr, false };
}

Series DataFrame::coalesce(std::vector<std::string> const& columns)
{
    std::vector<arrow::Datum> args(columns.size());
    std::transform(
        columns.begin(),
        columns.end(),
        args.begin(),
        [this](std::string const& key)
        { return m_array->GetColumnByName(key); });
    auto result = arrow::compute::CallFunction("coalesce", args);
    if (result.ok())
    {
        return { result.MoveValueUnsafe().make_array(), false };
    }
    throwOnNotOkStatus(result.status());
    return { nullptr, false };
}

class GroupBy DataFrame::group_by(const std::string& key) const
{
    return { { m_array->GetColumnByName(key) }, m_array };
}

DataFrame GroupBy::Compute::agg(
    std::vector<arrow::compute::Aggregate> const& funcs,
    Series const& key,
    std::vector<arrow::Datum> const& columns,
    arrow::FieldVector const& fields,
    bool skip_null)
{
    auto result =
        arrow::compute::internal::GroupBy(columns, { key.array() }, funcs);
    if (result.ok())
    {
        auto array = result.MoveValueUnsafe().array_as<arrow::StructArray>();
        auto new_index = key.unique().m_array;
        return { arrow::schema(fields),
                 new_index->length(),
                 array->fields(),
                 new_index };
    }
    throw std::runtime_error(result.status().ToString());
}

DataFrame GroupBy::Compute::agg(
    const std::vector<std::string>& fnc,
    bool skip_null)
{

    std::vector<std::pair<std::shared_ptr<arrow::Field>, DataFrame::ArrayType>>
        agg_result;

    int total_size = fnc.size() + columns.size();
    std::vector<arrow::compute::Aggregate> aggregations;
    std::vector<arrow::Datum> args;
    arrow::FieldVector fields;

    fields.reserve(total_size);
    aggregations.reserve(total_size);
    args.reserve(total_size);

    std::shared_ptr<arrow::compute::FunctionOptions> f =
        std::make_shared<arrow::compute::ScalarAggregateOptions>(skip_null);

    int field_idx = 0;
    for (auto const& col : column_data)
    {
        auto field = columns[field_idx++];
        for (auto const& name : fnc)
        {
            aggregations.emplace_back(
                arrow::compute::Aggregate{ "hash_" + name, f });
            args.emplace_back(col);
            fields.emplace_back(
                arrow::field(field->name() + "_" + name, field->type()));
        }
    }

    return agg(aggregations, key_data, args, fields, skip_null).sort_index();
}

GroupBy::Compute GroupBy::operator[](
    std::vector<std::string> const& columns_str)
{
    std::vector<arrow::Datum> args(columns_str.size());
    arrow::FieldVector columns;
    auto fields = rb->schema()->fields();
    std::ranges::transform(
        columns_str,
        args.begin(),
        [&, this](auto const& k)
        {
            auto arr = rb->GetColumnByName(k);
            columns.emplace_back(arrow::field(k, arr->type()));
            return arr;
        });
    return Compute{ columns, args, { key, false } };
}


}