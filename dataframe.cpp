//
// Created by dewe on 12/27/22.
//

#include "dataframe.h"
#include <arrow/api.h>
#include <arrow/compute/exec.h>
#include <arrow/io/api.h>
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
#include "arrow/compute/exec/aggregate.h"
#include "arrow/compute/registry.h"
#include "arrow/table.h"
#include "arrow/type.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/int_util_overflow.h"
#include "arrow/util/key_value_metadata.h"
#include "arrow/util/vector.h"
#include "datetimelike.h"
#include "filesystem"
#include "group_by.h"
#include "macros.h"

namespace pd {

DataFrame::DataFrame(
    const std::shared_ptr<arrow::RecordBatch>& table,
    const std::shared_ptr<arrow::Array>& _index)
    : NDArray(table, _index)
{
    if (not _index)
    {
        if (table)
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
    std::vector<std::string> const& columns = {})
    : NDArray<DataFrame>(nullptr)
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
        if (index != -1)
        {
            auto field = schemas->GetFieldByName(old)->WithName(new_);
            schemas = schemas->SetField(index, field).MoveValueUnsafe();
        }
        else
        {
            throw std::runtime_error(
                "trying to rename with invalid field: " + old);
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

std::unordered_map<std::string, pd::Scalar> DataFrame::operator[](
    int64_t row) const
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
    return { result.array_as<arrow::StructArray>(),
             std::vector<std::string>{ "year", "month", "day" } };
}

Scalar DataFrame::at(int64_t row, int64_t col) const
{

    if (row < 0)
    {
        throw std::runtime_error("@DataFrame::at() row must be >= 0");
    }

    if (col < num_columns())
    {
        auto ptr = m_array->column(col);
        auto result = ptr->GetScalar(row);
        if (result.ok())
        {
            return result.MoveValueUnsafe();
        }
        throw std::runtime_error(
            std::to_string(row) + " is an invalid row index for column " +
            columnNames().at(col));
    }

    throw std::runtime_error(
        std::to_string(col) + " is not a valid column index");
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
    if (m_array == nullptr)
    {
        return {};
    }

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
            {
                return std::find(
                           valid_types.begin(),
                           valid_types.end(),
                           field->type()) == valid_types.end();
            }))
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
    std::vector<arrow::ScalarVector> quantiles(
        percentiles_list.size(),
        arrow::ScalarVector{ N });
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
        fields.emplace_back(arrow::field(
            std::to_string(int((*p++) * 100)).append("%"),
            common_type));
    }

    data.emplace_back(arrow::ScalarArray::Make(max));
    fields.emplace_back(arrow::field("max", common_type));

    return { arrow::schema(fields),
             static_cast<int64_t>(indexes.size()),
             data,
             indexesArray };
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
    return (m_array == nullptr) ? std::vector<std::string>{} :
                                  m_array->schema()->field_names();
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

GroupBy DataFrame::group_by(const std::string& key) const
{
    return { key, *this };
}

//arrow::Result<pd::DataFrame> GroupBy::apply(
//    std::function<std::shared_ptr<arrow::Scalar>(pd::DataFrame const&)> fn)
//{
//    auto schema = df.m_array->schema();
//    auto args = schema->field_names();
//    auto N = unique_value->length();
//
//    auto fv = schema->fields();
//    arrow::ArrayDataVector arr(args.size());
//
//    std::vector<std::shared_ptr<arrow::Scalar>> scalars(N);
//
//    tbb::parallel_for(
//        tbb::blocked_range<size_t>(0, N),
//        [&](const tbb::blocked_range<size_t>& r)
//        {
//            for (size_t i = r.begin(); i != r.end(); ++i)
//            {
//                auto key = unique_value->GetScalar(long(i)).MoveValueUnsafe();
//                auto const& group = groups.at(key);
//                auto index = indexChunk[key];
//                auto rows = index->length();
//
//                scalars[i] =
//                    fn(pd::DataFrame{ schema, rows, group, index });
//            }
//        });
//
//    auto builder = arrow::MakeBuilder(scalars.back()->type).MoveValueUnsafe();
//    builder->AppendScalars(scalars);
//    builder->FinishInternal(&arr.back());
//
//    return pd::DataFrame(arrow::schema(fv), long(N), arr);
//}

GROUPBY_NUMERIC_AGG(mean, double)
GROUPBY_NUMERIC_AGG(approximate_median, double)
GROUPBY_NUMERIC_AGG(stddev, double)
GROUPBY_NUMERIC_AGG(tdigest, double)
GROUPBY_NUMERIC_AGG(variance, double)

GROUPBY_NUMERIC_AGG(count, int64_t)
GROUPBY_NUMERIC_AGG(count_distinct, int64_t)
//GROUPBY_NUMERIC_AGG(index, int64_t)

GROUPBY_AGG(max)
GROUPBY_AGG(min)
GROUPBY_AGG(sum)

//GROUPBY_AGG(product, int64_t)
//GROUPBY_AGG(quantile, int64_t)


arrow::Status GroupBy::processEach(
    std::unique_ptr<arrow::compute::Grouper> const& grouper,
    std::shared_ptr<arrow::ListArray> const& groupings,
    std::shared_ptr<arrow::Array> const& column)
{
    using namespace arrow;
    using namespace arrow::compute;

    ARROW_ASSIGN_OR_RAISE(
        auto grouped_argument,
        Grouper::ApplyGroupings(*groupings, *column));

    for (int64_t i_group = 0; i_group < grouper->num_groups(); ++i_group)
    {
        ARROW_ASSIGN_OR_RAISE( std::shared_ptr<arrow::Scalar> keyScalar, unique_value->GetScalar(i_group));

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

    ARROW_ASSIGN_OR_RAISE(
        auto grouped_argument,
        Grouper::ApplyGroupings(*groupings, *df.indexArray()));

    for (int64_t i_group = 0; i_group < grouper->num_groups(); ++i_group)
    {
        ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::Scalar>  keyScalar, unique_value->GetScalar(i_group));
        indexChunk[keyScalar] = grouped_argument->value_slice(i_group);
    }
    return arrow::Status::OK();
}

arrow::Status GroupBy::makeGroups()
{
    using namespace arrow;
    using namespace arrow::compute;

    auto schema = df.array()->schema();

    ARROW_ASSIGN_OR_RAISE(
        auto key_batch,
        ExecBatch::Make(std::vector<Datum>{ df[keyStr].array() }));

    ARROW_ASSIGN_OR_RAISE(auto grouper, Grouper::Make(key_batch.GetTypes()));

    ARROW_ASSIGN_OR_RAISE(
        Datum id_batch,
        grouper->Consume(ExecSpan(key_batch)));

    ARROW_ASSIGN_OR_RAISE(
        auto groupings,
        Grouper::MakeGroupings(
            *id_batch.array_as<UInt32Array>(),
            grouper->num_groups()));

    ARROW_ASSIGN_OR_RAISE(auto uniques, grouper->GetUniques());
    unique_value = uniques.values[0].make_array();

    processIndex(grouper, groupings);

    for (auto const& col : df.m_array->columns())
    {
        processEach(grouper, groupings, col);
    }

    return arrow::Status::OK();
}

}