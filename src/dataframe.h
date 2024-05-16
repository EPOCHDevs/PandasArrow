//
// Created by dewe on 12/27/22.
//
#pragma once
#include "filesystem"
#include "ndframe.h"
#include "set"


namespace pd {

constexpr int PD_MAX_ROW_TO_PRINT = { 7000 };
constexpr int PD_MAX_COL_TO_PRINT = { 100 };

template<class T>
concept LiteralType = std::is_floating_point_v<T> || std::is_integral_v<T> || std::is_same_v<T, std::string>;

template<class T> requires LiteralType<T>
using Row = std::pair<std::string, std::vector<T>>;

template<class T> requires LiteralType<T>
std::pair<std::string, std::vector<T>> GetRow(std::string const& columnName, std::initializer_list<T> const& value)
{
    return { columnName, std::vector<T>{ value.begin(), value.end() } };
}

template<class T> requires LiteralType<T>
std::pair<std::string, std::vector<T>> GetRow(std::function<std::string()> const& columnName, std::initializer_list<T> const& value)
{
    return GetRow(columnName(), value);
}

template<class T> requires LiteralType<T>
std::pair<std::string, std::vector<T>> GetRow(std::string const& columnName, std::vector<T> const& value)
{
    return { columnName, value };
}

template<class T> requires LiteralType<T>
std::pair<std::string, std::vector<T>> GetRow(std::function<std::string()> const& columnName, std::vector<T> const& value)
{
    return GetRow(columnName(), value );
}

template<class T> requires LiteralType<T>
std::pair<std::string, std::vector<T>> GetRow(std::string const& columnName, T const& value)
{
    return { columnName, std::vector<T>{ value } };
}

template<class T> requires LiteralType<T>
std::pair<std::string, std::vector<T>> GetRow(std::string const& columnName, pd::Scalar const& value)
{
    return GetRow(columnName, value.as<T>());
}

template<class T> requires LiteralType<T>
std::pair<std::string, std::vector<T>> GetRow(std::function<std::string()> const& columnName, T const& value)
{
    return { columnName(), std::vector<T>{ value } };
}

template<class T> requires LiteralType<T>
std::pair<std::string, std::vector<T>> GetRow(std::function<std::string()> const& columnName, pd::Scalar const& value)
{
    return GetRow(columnName, value.as<T>());
}


constexpr const char * RESERVED_INDEX_NAME = "__INDEX_NAME__";
class DataFrame : public NDFrame<DataFrame>
{
public:
    using ArrayType = std::shared_ptr<arrow::RecordBatch>;
    using Shape = std::array<int64_t, 2>;

    template<class MapType>
    size_t GetTableRowSize(MapType const& table);

    DataFrame()
    {
        m_index = pd::ReturnOrThrowOnFailure(arrow::MakeEmptyArray(arrow::null()));
    }

    DataFrame(std::shared_ptr<arrow::RecordBatch> const& table, std::shared_ptr<arrow::Array> const& _index = nullptr);

    DataFrame(
        std::shared_ptr<arrow::Schema> const& schemas,
        int64_t num_rows,
        std::vector<std::shared_ptr<arrow::ArrayData>> const& table,
        std::shared_ptr<arrow::Array> const& _index = nullptr)
        : NDFrame<DataFrame>(arrow::RecordBatch::Make(schemas, num_rows, table), _index)
    {
    }

    DataFrame(
        std::shared_ptr<arrow::Schema> const& schemas,
        int64_t num_rows,
        std::vector<std::shared_ptr<arrow::Array>> const& table,
        std::shared_ptr<arrow::Array> const& _index = nullptr)
        : NDFrame<DataFrame>(arrow::RecordBatch::Make(schemas, num_rows, table), _index)
    {
    }

    template<class... ColumnTypes, size_t N = std::tuple_size_v<std::tuple<ColumnTypes...>>>
    DataFrame(
        std::array<std::string, N> columns,
        std::shared_ptr<arrow::Array> const& _index,
        std::vector<ColumnTypes>&&... columnData);

    template<class... ColumnTypes, size_t N = std::tuple_size_v<std::tuple<ColumnTypes...>>>
    DataFrame(
        std::shared_ptr<arrow::Array> const& _index,
        Row<ColumnTypes> &&... columnData);

    template<class T>
    DataFrame(
        std::vector<std::vector<T>> const& table,
        std::vector<std::string> columns = {},
        std::shared_ptr<arrow::Array> const& _index = nullptr);

    template<template<typename...> class MapType, typename V>
    DataFrame(MapType<std::string, V> const& table, std::shared_ptr<arrow::Array> const& index = nullptr);

    DataFrame(ArrayTable const& table, std::shared_ptr<arrow::Array> const& index = nullptr);

    DataFrame(std::shared_ptr<arrow::StructArray> const& arr, std::vector<std::string> const& columns);

    inline auto shape() const
    {
        return (m_array == nullptr) ? std::array<int64_t, 2>{ 0, 0 } :
                                      std::array<int64_t, 2>{ num_rows(), num_columns() };
    }


    inline ArrayPtr fieldArray() const
    {
        return arrow::ArrayT<std::string>::Make(columnNames());
    }

    TablePtr toTable(std::optional<std::string> const& index_name = {}) const;

    template<class T>
    static std::pair<arrow::FieldVector, arrow::ArrayVector> makeFieldArrayPair(
        std::vector<std::vector<T>> const& table,
        std::vector<std::string> const& columns);

    template<template<typename...> class MapType, typename V>
    static std::pair<arrow::FieldVector, arrow::ArrayVector> makeFieldArrayPair(MapType<std::string, V> const& table);

    template<
        size_t N,
        size_t I = 0,
        class... PrimitiveTypes,
        typename TupleT = std::tuple<PrimitiveTypes...>,
        typename TupleVectorT = std::tuple<std::vector<PrimitiveTypes>...>>
    static void makeFieldArrayPairT(
        TupleVectorT&& columnData,
        arrow::ArrayVector& arrays,
        arrow::FieldVector& fields,
        std::array<std::string, N> const& columns);

    template<
        size_t N,
        size_t I = 0,
        class... PrimitiveTypes,
        typename TupleT = std::tuple<PrimitiveTypes...>,
        typename TupleVectorT = std::tuple<std::pair<std::string, std::vector<PrimitiveTypes>>...>>
    static void makeFieldArrayPairT(TupleVectorT&& columnData, arrow::ArrayVector& arrays, arrow::FieldVector& fields);

    DataFrame transpose() const;

    DataFrame& rename(std::unordered_map<std::string, std::string> const& columns);

    bool contains(std::string const& column) const;

    void add_prefix(std::string const& prefix, std::set<std::string> const& excludes = {});

    void add_suffix(std::string const& suffix, std::set<std::string> const& excludes = {});

    static std::vector<std::string> makeDefaultColumnNames(size_t N);

    inline int64_t num_rows() const
    {
        return m_array->num_rows();
    }

    inline int64_t num_columns() const
    {
        return m_array->num_columns();
    }

    template<size_t N>
    std::array<std::string, N> makeDefaultColumnNames();

    arrow::FieldVector dtypes() const;

    std::vector<std::string> columns() const;

    friend std::ostream& operator<<(std::ostream& a, DataFrame const& b);

    DataFrame describe(bool include_all = true, bool percentiles = false);

    static DataFrame readParquet(std::filesystem::path const& path);
    static DataFrame readCSV(std::filesystem::path const& path);

    arrow::Status toParquet(std::filesystem::path const& filepath, const std::string& indexField="index");
    arrow::Status toCSV(std::filesystem::path const& filepath, const std::string& indexField="index") const;

    // indexer
    class Series operator[](std::string const& column) const;

    pd::DataFrame add_column(std::string const& newColumn, pd::ArrayPtr const& ptr) const;

    void add_column(std::string const& newColumn, pd::ArrayPtr const& ptr);

    void add_column(std::string const& newColumn, pd::Series const& ptr);

    DataFrame operator[](std::vector<std::string> const& columns) const;
    DataFrame operator[](int64_t row) const;

    DataFrame operator[](pd::Series const& filter) const;

    Scalar at(int64_t row, int64_t col) const;

    Scalar at(int64_t row, std::string const& col) const
    {
        auto idx = m_array->schema()->GetFieldIndex(col);
        if (idx == -1)
        {
            throw std::runtime_error(col + " is not a valid column");
        }
        return at(row, idx);
    }

    pd::DataFrame reset_index(std::string const& columnName, bool drop) const;

    pd::DataFrame reindex(std::shared_ptr<arrow::Array> const& newIndex,
                          const std::optional<Scalar>& fillValue=std::nullopt) const noexcept;

    pd::DataFrame reindexAsync(std::shared_ptr<arrow::Array> const& newIndex,
                               const std::optional<Scalar>& fillValue=std::nullopt) const noexcept;

    Scalar at(std::shared_ptr<arrow::Scalar> const& row, std::string const& col) const
    {
        return at(
            arrow::compute::Index(m_index, arrow::compute::IndexOptions(row))->scalar_as<arrow::Int64Scalar>().value,
            col);
    }

    template<typename T>
    requires(not std::integral<T>) Scalar at(T const& row, std::string const& col)
    const
    {
        return at(arrow::MakeScalar(row), col);
    }

    DataFrame ffill() const;
    DataFrame bfill() const;

    [[nodiscard]] DataFrame is_null() const;
    [[nodiscard]] DataFrame is_valid() const;
    [[nodiscard]] DataFrame is_finite() const;
    [[nodiscard]] DataFrame is_infinite() const;

    void drop(std::vector<std::string> const&);

    inline void drop(std::string const& column) {
        drop(std::vector{column});
    }

    DataFrame drop_na() const;

    DataFrame slice(int offset) const;
    DataFrame slice(int offset, std::vector<std::string> const& columns) const;

    DataFrame slice(Slice, std::vector<std::string> const& columns) const;
    DataFrame slice(DateTimeSlice, std::vector<std::string> const& columns) const;
    DataFrame slice(Slice) const;
    DataFrame timestamp_slice(Slice const& slice) const;
    DataFrame slice(DateTimeSlice) const;

    [[nodiscard]] Series index() const;

    [[nodiscard]] DataFrame head(int length = 5) const;
    [[nodiscard]] DataFrame tail(int length = 5) const;

    DataFrame setIndex(std::string const& column_name);

    DataFrame setIndex(std::shared_ptr<arrow::Array> const& index) const
    {
        auto newClone = *this;
        newClone.m_index = index;
        return newClone;
    }

    DataFrame indexAsDateTime() const;

    DataFrame setColumns(std::vector<std::string> const& column_names);

    [[nodiscard]] pd::Series forAxis(std::string const& functionName, pd::AxisType) const;

    [[nodiscard]] Scalar sum() const;

    [[nodiscard]] Series sum(pd::AxisType axis) const;

    [[nodiscard]] Series count(pd::AxisType axis) const;

    Series std(pd::AxisType axis) const;
    Series var(pd::AxisType axis) const;
    Series max(pd::AxisType axis) const;
    Series min(pd::AxisType axis) const;

    bool equals_(DataFrame const& other) const override
    {
        return m_array->Equals(*other.m_array) && m_index->Equals(other.indexArray());
    }

    template<typename T>
    requires(not std::same_as<DataFrame, T>) bool equals(std::vector<T> const& a) const;

    template<typename T>
    requires(not std::same_as<DataFrame, T>) bool approx_equals(std::vector<T> const& a) const;

    DataFrame operator*(DataFrame const& s) const;
    friend DataFrame operator*(Series const& s, DataFrame const& df);

    DataFrame operator/(DataFrame const& s) const;
    friend DataFrame operator/(Series const& s, DataFrame const& df);

    DataFrame operator+(DataFrame const& s) const;
    friend DataFrame operator+(Series const& s, DataFrame const& df);

    DataFrame operator-(DataFrame const& s) const;
    friend DataFrame operator-(Series const& s, DataFrame const& df);

    DataFrame operator|(DataFrame const& s) const;
    friend DataFrame operator|(Series const& s, DataFrame const& df);

    DataFrame operator&(DataFrame const& s) const;
    friend DataFrame operator&(Series const& s, DataFrame const& df);

    DataFrame operator^(DataFrame const& s) const;
    friend DataFrame operator^(Series const& s, DataFrame const& df);

    DataFrame operator<<(DataFrame const& s) const;
    friend DataFrame operator<<(Series const& s, DataFrame const& df);

    DataFrame operator>>(DataFrame const& s) const;
    friend DataFrame operator>>(Series const& s, DataFrame const& df);

    DataFrame operator>(DataFrame const& s) const;
    friend DataFrame operator>(Series const& s, DataFrame const& df);

    DataFrame operator>=(DataFrame const& s) const;
    friend DataFrame operator>=(Series const& s, DataFrame const& df);

    DataFrame operator<(DataFrame const& s) const;
    friend DataFrame operator<(Series const& s, DataFrame const& df);

    DataFrame operator<=(DataFrame const& s) const;
    friend DataFrame operator<=(Series const& s, DataFrame const& df);

    DataFrame operator==(DataFrame const& s) const;
    friend DataFrame operator==(Series const& s, DataFrame const& df);

    DataFrame operator!=(DataFrame const& s) const;
    friend DataFrame operator!=(Series const& s, DataFrame const& df);

    DataFrame operator&&(DataFrame const& s) const;
    friend DataFrame operator&&(Series const& s, DataFrame const& df);

    DataFrame operator||(DataFrame const& s) const;
    friend DataFrame operator||(Series const& s, DataFrame const& df);

    virtual DataFrame operator+(Series const& s) const;

    virtual DataFrame operator+(Scalar const& s) const;

    DataFrame operator-(Series const& s) const;

    DataFrame operator-(Scalar const& s) const;

    DataFrame unary(std::string const& functionName) const;

    inline DataFrame operator-() const
    {
        return unary("negate");
    }

    DataFrame operator~() const
    {
        return unary("bit_wise_not");
    }

    DataFrame operator/(Series const& a) const;

    DataFrame operator/(Scalar const& a) const;

    DataFrame operator==(Scalar const& a) const;

    DataFrame operator==(Series const& a) const;

    DataFrame operator!=(Scalar const& a) const;

    DataFrame operator!=(Series const& a) const;

    DataFrame operator<=(Scalar const& a) const;

    DataFrame operator<=(Series const& a) const;

    DataFrame operator>=(Scalar const& a) const;

    DataFrame operator>=(Series const& a) const;

    DataFrame operator>(Scalar const& a) const;

    DataFrame operator>(Series const& a) const;

    DataFrame operator<(Scalar const& a) const;

    DataFrame operator<(Series const& a) const;

    DataFrame operator|(Scalar const& a) const;

    DataFrame operator|(Series const& a) const;

    DataFrame operator&(Scalar const& a) const;

    DataFrame operator&(Series const& a) const;

    DataFrame operator||(Scalar const& a) const;

    DataFrame operator||(Series const& a) const;

    DataFrame operator&&(Scalar const& a) const;

    DataFrame operator&&(Series const& a) const;

    DataFrame operator^(Scalar const& a) const;

    DataFrame operator^(Series const& a) const;

    DataFrame operator<<(Scalar const& s) const;

    DataFrame operator<<(Series const& a) const;

    DataFrame operator>>(Scalar const& s) const;

    DataFrame operator>>(Series const& a) const;

    DataFrame operator*(Series const& a) const;

    DataFrame operator*(Scalar const& a) const;

    friend DataFrame operator+(Scalar const& a, DataFrame const& b);

    friend DataFrame operator/(Scalar const& a, DataFrame const& b);

    friend DataFrame operator*(Scalar const& a, DataFrame const& b);

    friend DataFrame operator-(Scalar const& a, DataFrame const& b);

    friend DataFrame operator|(Scalar const& a, DataFrame const& b);

    friend DataFrame operator&(Scalar const& a, DataFrame const& b);

    friend DataFrame operator^(Scalar const& a, DataFrame const& b);

    friend DataFrame operator||(Scalar const& a, DataFrame const& b);

    friend DataFrame operator&&(Scalar const& a, DataFrame const& b);

    friend DataFrame operator<<(Scalar const& a, DataFrame const& b);

    friend DataFrame operator>>(Scalar const& a, DataFrame const& b);

    friend DataFrame operator<(Scalar const& a, DataFrame const& b);

    friend DataFrame operator<=(Scalar const& a, DataFrame const& b);

    friend DataFrame operator>(Scalar const& a, DataFrame const& b);

    friend DataFrame operator>=(Scalar const& a, DataFrame const& b);

    friend DataFrame operator==(Scalar const& a, DataFrame const& b);

    friend DataFrame operator!=(Scalar const& a, DataFrame const& b);

    [[nodiscard]] Series argsort(std::vector<std::string> const& fields, bool ascending) const;

    [[nodiscard]] DataFrame sort_index(bool ascending = true, bool ignore_index = false);

    [[nodiscard]] DataFrame sort_values(
        std::vector<std::string> const& by,
        bool ascending = true,
        bool ignore_index = false);

    [[nodiscard]] class GroupBy group_by(std::string const&) const;
    [[nodiscard]] class Resampler resample(
        std::string const& rule,
        bool closed_right = false,
        bool label_right = false,
        TimeGrouperOrigin const& origin = {},
        time_duration const& offset = time_duration(),
        std::string const& tz = "") const;

    [[nodiscard]] Series coalesce();
    [[nodiscard]] Series coalesce(std::vector<std::string> const& columns);

    std::vector<std::string> columnNames() const;
    Series mean(AxisType axis) const;
    DataFrame slice(int offset, int64_t length) const;
};

template<class... ColumnTypes, size_t N>
DataFrame::DataFrame(
    std::array<std::string, N> columns,
    std::shared_ptr<arrow::Array> const& _index,
    std::vector<ColumnTypes>&&... columnData)
    : NDFrame<DataFrame>(std::get<0>(std::forward_as_tuple(columnData...)).size(), _index)
{

    if (m_index->length() == 0)
    {
        throw std::runtime_error("Cannot Create DataFrame with empty columns");
    }

    auto&& tuple = std::forward_as_tuple(columnData...);
    int64_t nRows = std::get<0>(tuple).size();

    if (columns.empty())
    {
        columns = makeDefaultColumnNames<N>();
    }

    arrow::ArrayVector arrays(N);
    arrow::FieldVector fields(N);

    makeFieldArrayPairT<N, 0, ColumnTypes...>(std::forward_as_tuple(columnData...), arrays, fields, std::move(columns));

    m_array = arrow::RecordBatch::Make(arrow::schema(fields), nRows, std::move(arrays));
}

template<class... ColumnTypes, size_t N>
DataFrame::DataFrame(
    std::shared_ptr<arrow::Array> const& _index,
    Row<ColumnTypes>&&... columnData)
    : NDFrame<DataFrame>(std::get<0>(std::forward_as_tuple(columnData...)).second.size(), _index)
{

    int64_t nRows = m_index->length();
    if (nRows == 0)
    {
        throw std::runtime_error("Cannot Create DataFrame with empty columns");
    }

    arrow::ArrayVector arrays(N);
    arrow::FieldVector fields(N);

    makeFieldArrayPairT<N, 0, ColumnTypes...>(std::forward_as_tuple(columnData...), arrays, fields);

    m_array = arrow::RecordBatch::Make(arrow::schema(fields), nRows, std::move(arrays));
}

template<class T>
DataFrame::DataFrame(
    std::vector<std::vector<T>> const& table,
    std::vector<std::string> columns,
    std::shared_ptr<arrow::Array> const& _index)
    : NDFrame<DataFrame>(table.back().size(), _index)
{

    if (m_index->length() == 0)
    {
        throw std::runtime_error("Cannot Create DataFrame with empty columns");
    }

    int nRows = table.back().size();

    if (columns.empty())
    {
        columns = makeDefaultColumnNames(table.size());
    }

    auto [fields, arrays] = makeFieldArrayPair(table, std::move(columns));

    m_array = arrow::RecordBatch::Make(arrow::schema(fields), nRows, std::move(arrays));
}

template<template<typename...> class MapType, typename V>
DataFrame::DataFrame(MapType<std::string, V> const& table, std::shared_ptr<arrow::Array> const& index)
    : NDFrame<DataFrame>(GetTableRowSize(table), index)
{

    if (m_index->length() == 0)
    {
        throw std::runtime_error("Cannot Create DataFrame with empty columns");
    }

    int nRows = m_index->length();

    auto [fields, arrays] = makeFieldArrayPair(table);

    m_array = arrow::RecordBatch::Make(arrow::schema(fields), nRows, std::move(arrays));
}

template<class T>
requires std::is_scalar_v<T> T getDefault(T scalarV)
{
    return scalarV;
}

template<class T>
T getDefault(std::vector<T> const&)
{
    return T{};
}

template<class T>
std::pair<arrow::FieldVector, arrow::ArrayVector> DataFrame::makeFieldArrayPair(
    std::vector<std::vector<T>> const& table,
    std::vector<std::string> const& columns)
{
    arrow::ArrayVector arrays(table.size());
    arrow::FieldVector fields;
    fields.reserve(table.size());

    std::transform(
        table.begin(),
        table.end(),
        columns.begin(),
        arrays.begin(),
        [&fields](std::vector<T> const& column, std::string const& name)
        {
            typename arrow::CTypeTraits<T>::BuilderType builder;
            arrow::Status status;
            std::vector<uint8_t> cacheForStringArrayNullBitMap;

            if constexpr (std::is_same_v<T, std::string>)
            {
                cacheForStringArrayNullBitMap = makeValidFlags(column);
                status = builder.AppendValues(column, cacheForStringArrayNullBitMap.data());
            }
            else
            {
                status = builder.AppendValues(column, makeValidFlags(column));
            }

            if (status.ok())
            {
                auto result = builder.Finish();
                if (result.ok())
                {
                    fields.push_back(arrow::field(name, arrow::CTypeTraits<T>::type_singleton()));
                    return result.MoveValueUnsafe();
                }
                throw std::runtime_error(result.status().ToString());
            }
            throw std::runtime_error(status.ToString());
        });

    return { fields, arrays };
}

template<template<typename...> class MapType, typename V>
std::pair<arrow::FieldVector, arrow::ArrayVector> DataFrame::makeFieldArrayPair(MapType<std::string, V> const& table)
{
    constexpr bool isScalar = std::is_scalar_v<V>;
    using T = decltype(getDefault(V{}));

    arrow::ArrayVector arrays(table.size());
    arrow::FieldVector fields;
    fields.reserve(table.size());

    std::transform(
        table.begin(),
        table.end(),
        arrays.begin(),
        [&fields](std::pair<std::string, V> const& columnItem)
        {
            typename arrow::CTypeTraits<T>::BuilderType builder;

            auto [name, column] = columnItem;

            arrow::Status status;
            std::vector<uint8_t> cacheForStringArrayNullBitMap;

            if constexpr (isScalar)
            {
                status = isValid(column) ? builder.Append(column) : builder.AppendNull();
            }
            else
            {
                if constexpr (std::is_same_v<T, std::string>)
                {
                    cacheForStringArrayNullBitMap = makeValidFlags(column);
                    status = builder.AppendValues(column, cacheForStringArrayNullBitMap.data());
                }
                else
                {
                    status = builder.AppendValues(column, makeValidFlags(column));
                }
            }

            if (status.ok())
            {
                auto result = builder.Finish();
                if (result.ok())
                {
                    fields.push_back(arrow::field(name, arrow::CTypeTraits<T>::type_singleton()));
                    return result.MoveValueUnsafe();
                }
                throw std::runtime_error(result.status().ToString());
            }
            throw std::runtime_error(status.ToString());
        });

    return { fields, arrays };
}

template<size_t N, size_t I, class... PrimitiveTypes, typename TupleT, typename TupleVectorT>
void DataFrame::makeFieldArrayPairT(
    TupleVectorT&& columnData,
    arrow::ArrayVector& arrays,
    arrow::FieldVector& fields,
    std::array<std::string, N> const& columns)
{
    if constexpr (I < N)
    {
        using T = std::tuple_element_t<I, TupleT>;
        using Type = arrow::CTypeTraits<T>;

        typename Type::BuilderType builder;
        auto column = std::get<I>(columnData);

        std::vector<uint8_t> cacheForStringArrayNullBitMap;
        if constexpr (std::is_same_v<T, std::string>)
        {
            cacheForStringArrayNullBitMap = makeValidFlags(column);
            ThrowOnFailure(builder.AppendValues(column, cacheForStringArrayNullBitMap.data()));
        }
        else
        {
            ThrowOnFailure(builder.AppendValues(column, makeValidFlags(column)));
        }

        arrays[I] = ReturnOrThrowOnFailure(builder.Finish());

        fields[I] = arrow::field(columns[I], Type::type_singleton());
        makeFieldArrayPairT<N, I + 1, PrimitiveTypes...>(
            std::forward<TupleVectorT>(columnData),
            arrays,
            fields,
            columns);
    }
}

template<class MapType>
size_t DataFrame::GetTableRowSize(MapType const& table)
{
    using T = typename MapType::mapped_type;
    if constexpr (std::is_scalar_v<T>)
    {
        return 1;
    }
    else
    {
        for (auto const& [_, value] : table)
        {
            if constexpr (std::is_same_v<T, ArrayPtr>)
            {
                return not value ? 0 : value->length();
            }
            else
            {
                return value.empty() ? 0 : value.size();
            }
        }
    }
    return 0;
}

template<size_t N, size_t I, class... PrimitiveTypes, typename TupleT, typename TupleVectorT>
void DataFrame::makeFieldArrayPairT(TupleVectorT&& columnData, arrow::ArrayVector& arrays, arrow::FieldVector& fields)
{
    if constexpr (I < N)
    {
        using T = std::tuple_element_t<I, TupleT>;
        using Type = arrow::CTypeTraits<T>;

        typename Type::BuilderType builder;
        auto [name, column] = std::get<I>(columnData);

        arrow::Status status;
        std::vector<uint8_t> cacheForStringArrayNullBitMap;
        if constexpr (std::is_same_v<T, std::string>)
        {
            cacheForStringArrayNullBitMap = makeValidFlags(column);
            ThrowOnFailure(builder.AppendValues(column, cacheForStringArrayNullBitMap.data()));
        }
        else
        {
            ThrowOnFailure(builder.AppendValues(column, makeValidFlags(column)));
        }

        arrays[I] = ReturnOrThrowOnFailure(builder.Finish());

        fields[I] = arrow::field(name, Type::type_singleton());
        makeFieldArrayPairT<N, I + 1, PrimitiveTypes...>(std::forward<TupleVectorT>(columnData), arrays, fields);
    }
}

template<size_t N>
std::array<std::string, N> DataFrame::makeDefaultColumnNames()
{
    std::array<std::string, N> columns;
    for (size_t i = 0; i < N; i++)
    {
        columns[i] = std::to_string(i);
    }
    return columns;
}

template<typename T>
requires(not std::same_as<DataFrame, T>) bool DataFrame::equals(std::vector<T> const& a) const
{
    return NDFrame<DataFrame>::equals_(DataFrame(a));
}

template<typename T>
requires(not std::same_as<DataFrame, T>) bool DataFrame::approx_equals(std::vector<T> const& a) const
{
    return NDFrame<DataFrame>::approx_equals_(DataFrame(a));
}
} // namespace pd