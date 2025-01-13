//
// Created by dewe on 12/27/22.
//
#pragma once
#include "filesystem"
#include "series.h"
#include "set"


namespace pd {

#define PD_MAX_ROW_TO_PRINT 1000
#define PD_MAX_COL_TO_PRINT 100

    template<class T>
    concept LiteralType = std::is_floating_point_v<T> || std::is_integral_v<T> || std::is_same_v<T, std::string>;

    template<class T> requires LiteralType<T>
    using Row = std::pair<std::string, std::vector<T>>;

    template<class T>
    requires LiteralType<T>
    std::pair<std::string, std::vector<T>>
    GetRow(std::string const &columnName, std::initializer_list<T> const &value) {
        return {columnName, std::vector<T>{value.begin(), value.end()}};
    }

    template<class T>
    requires LiteralType<T>
    std::pair<std::string, std::vector<T>>
    GetRow(std::function<std::string()> const &columnName, std::initializer_list<T> const &value) {
        return GetRow(columnName(), value);
    }

    template<class T>
    requires LiteralType<T>
    std::pair<std::string, std::vector<T>> GetRow(std::string const &columnName, std::vector<T> const &value) {
        return {columnName, value};
    }

    template<class T>
    requires LiteralType<T>
    std::pair<std::string, std::vector<T>>
    GetRow(std::function<std::string()> const &columnName, std::vector<T> const &value) {
        return GetRow(columnName(), value);
    }

    template<class T>
    requires LiteralType<T>
    std::pair<std::string, std::vector<T>> GetRow(std::string const &columnName, T const &value) {
        return {columnName, std::vector<T>{value}};
    }

    template<class T>
    requires LiteralType<T>
    std::pair<std::string, std::vector<T>> GetRow(std::string const &columnName, pd::Scalar const &value) {
        return GetRow(columnName, value.as<T>());
    }

    template<class T>
    requires LiteralType<T>
    std::pair<std::string, std::vector<T>> GetRow(std::function<std::string()> const &columnName, T const &value) {
        return {columnName(), std::vector<T>{value}};
    }

    template<class T>
    requires LiteralType<T>
    std::pair<std::string, std::vector<T>>
    GetRow(std::function<std::string()> const &columnName, pd::Scalar const &value) {
        return GetRow(columnName, value.as<T>());
    }

    constexpr const char *RESERVED_INDEX_NAME = "__INDEX_NAME__";

    class DataFrame : public NDFrame<arrow::RecordBatch> {
    public:
        using Shape = std::array<int64_t, 2>;

        //<editor-fold desc="Constructors">
        DataFrame() {
            m_index = pd::ReturnOrThrowOnFailure(arrow::MakeEmptyArray(arrow::null()));
        }

        explicit DataFrame(std::shared_ptr<arrow::RecordBatch> const &table,
                           std::shared_ptr<arrow::Array> const &_index = nullptr);

        DataFrame(
                std::shared_ptr<arrow::Schema> const &schemas,
                int64_t num_rows,
                std::vector<std::shared_ptr<arrow::ArrayData>> const &table,
                std::shared_ptr<arrow::Array> const &_index = nullptr)
                : NDFrame<arrow::RecordBatch>(arrow::RecordBatch::Make(schemas, num_rows, table), _index) {
        }

        DataFrame(
                std::shared_ptr<arrow::Schema> const &schemas,
                int64_t num_rows,
                std::vector<std::shared_ptr<arrow::Array>> const &table,
                std::shared_ptr<arrow::Array> const &_index = nullptr)
                : NDFrame<arrow::RecordBatch>(arrow::RecordBatch::Make(schemas, num_rows, table), _index) {
        }

         DataFrame(    std::shared_ptr<arrow::Schema> const &schemas,
                               int64_t num_rows,
                               std::shared_ptr<arrow::ChunkedArray> const &table,
                               std::shared_ptr<arrow::Array> const &_index = nullptr)
                               :  NDFrame<arrow::RecordBatch>(arrow::RecordBatch::Make(schemas, num_rows, table->chunks()), _index) {}

        DataFrame Make(arrow::ArrayVector const &table) const;

        template<class... ColumnTypes, size_t N = std::tuple_size_v<std::tuple<ColumnTypes...>>>
        DataFrame(
                std::array<std::string, N> columns,
                std::shared_ptr<arrow::Array> const &_index,
                std::vector<ColumnTypes> &&...columnData);

        template<class... ColumnTypes, size_t N = std::tuple_size_v<std::tuple<ColumnTypes...>>>
        explicit DataFrame(
                std::shared_ptr<arrow::Array> const &_index,
                Row<ColumnTypes> &&...columnData);

        template<class T>
        explicit DataFrame(
                std::vector<std::vector<T>> const &table,
                std::vector<std::string> columns = {},
                std::shared_ptr<arrow::Array> const &_index = nullptr);

        template<template<typename...> class MapType, typename V>
        explicit DataFrame(MapType<std::string, V> const &table, std::shared_ptr<arrow::Array> const &index = nullptr);

        explicit DataFrame(ArrayTable const &table, std::shared_ptr<arrow::Array> const &index = nullptr);

        DataFrame(std::shared_ptr<arrow::StructArray> const &arr, std::vector<std::string> const &columns);
        //</editor-fold>

        //<editor-fold desc="Aggregation Functions">
        using NDFrame<arrow::RecordBatch>::all;
        using NDFrame<arrow::RecordBatch>::any;
        using NDFrame<arrow::RecordBatch>::median;
        using NDFrame<arrow::RecordBatch>::count;
        using NDFrame<arrow::RecordBatch>::count_na;
        using NDFrame<arrow::RecordBatch>::nunique;
        using NDFrame<arrow::RecordBatch>::first;
        using NDFrame<arrow::RecordBatch>::first_last;
        using NDFrame<arrow::RecordBatch>::index;
        using NDFrame<arrow::RecordBatch>::last;
        using NDFrame<arrow::RecordBatch>::max;
        using NDFrame<arrow::RecordBatch>::mean;
        using NDFrame<arrow::RecordBatch>::min;
        using NDFrame<arrow::RecordBatch>::min_max;
        using NDFrame<arrow::RecordBatch>::mode;
        using NDFrame<arrow::RecordBatch>::product;
        using NDFrame<arrow::RecordBatch>::quantile;
        using NDFrame<arrow::RecordBatch>::std;
        using NDFrame<arrow::RecordBatch>::sum;
        using NDFrame<arrow::RecordBatch>::tdigest;
        using NDFrame<arrow::RecordBatch>::var;
        using NDFrame<arrow::RecordBatch>::agg;

        [[nodiscard]] pd::Series forAxis(std::string const &functionName, pd::AxisType,
                                         const arrow::compute::FunctionOptions&) const;

        [[nodiscard]] pd::Series all(AxisType axis, bool skip_null = true) const;

        [[nodiscard]] pd::Series any(AxisType axis, bool skip_null = true) const;

        [[nodiscard]] pd::Series median(AxisType axis, bool skip_null = true) const;

        [[nodiscard]] pd::Series count(AxisType axis) const;

        [[nodiscard]] pd::Series count_na(AxisType axis) const;

        [[nodiscard]] pd::Series nunique(AxisType axis) const;

        [[nodiscard]] pd::Series first(AxisType axis, bool skip_null = true) const;

        [[nodiscard]] pd::Series last(AxisType axis,bool skip_null = true) const;

        // TODO: use max_element_wise
        [[nodiscard]] pd::Series max(AxisType axis, bool skip_null = true) const;

        [[nodiscard]] pd::Series mean(AxisType axis, bool skip_null = true) const;

        // TODO: use min_element_wise
        [[nodiscard]] pd::Series min(AxisType axis, bool skip_null = true) const;

        [[nodiscard]] pd::Series product(AxisType axis, bool skip_nulls) const;

        [[nodiscard]] pd::Series quantile(AxisType axis, double q = 0.5,
                                          arrow::compute::QuantileOptions::Interpolation interpolation =
                                          arrow::compute::QuantileOptions::Interpolation::LINEAR,
                                          bool skip_nulls = true, uint32_t min_count = 0) const;

        [[nodiscard]] pd::Series std(AxisType axis, int ddof = 1, bool skip_na = true) const;

        [[nodiscard]] pd::Series sum(AxisType axis, bool skip_null=true) const;

        [[nodiscard]] pd::Series tdigest(AxisType axis, double q = 0.5, uint32_t delta = 100,
                                         uint32_t buffer_size = 500, bool skip_nulls = true,
                                         uint32_t min_count = 0) const;

        [[nodiscard]] pd::Series var(AxisType axis, int ddof = 1, bool skip_na = true) const;
        //</editor-fold>

        //<editor-fold desc="Arithmetric Operation">
        [[nodiscard]] DataFrame abs() const;

        DataFrame operator+(DataFrame const &s) const;
        DataFrame operator+(Series const &s) const;
        DataFrame operator+(Scalar const &s) const;
        PANDAS_SCALAR_OVERRIDES(+, DataFrame)

        DataFrame operator/(DataFrame const &a) const;
        DataFrame operator/(Series const &a) const;
        DataFrame operator/(Scalar const &a) const;
        PANDAS_SCALAR_OVERRIDES(/, DataFrame)

        [[nodiscard]] DataFrame exp() const;

        virtual DataFrame operator*(DataFrame const &a) const;
        virtual DataFrame operator*(Series const &a) const;
        virtual DataFrame operator*(Scalar const &a) const;
        PANDAS_SCALAR_OVERRIDES(*, DataFrame)

        [[nodiscard]] DataFrame pow(double x) const;
        [[nodiscard]] DataFrame sign() const;
        [[nodiscard]] DataFrame sqrt() const;

        DataFrame operator-(DataFrame const &s) const;
        DataFrame operator-(Series const &s) const;
        DataFrame operator-(Scalar const &s) const;
        PANDAS_SCALAR_OVERRIDES(-, DataFrame)
        //</editor-fold>

        //<editor-fold desc="Indexing Functions">
        DataFrame operator[](Slice) const final;
        using NDFrame<arrow::RecordBatch>::operator[];

        Series operator[](std::string const &column) const;
        DataFrame operator[](std::vector<std::string> const &columns) const;
        DataFrame operator[](int64_t row) const;
        Scalar at(int64_t row, int64_t col) const;
        Scalar at(int64_t row, std::string const &col) const;

        DataFrame slice(int offset) const;
        DataFrame slice(int offset, std::vector<std::string> const &columns) const;
        DataFrame slice(Slice, std::vector<std::string> const &columns) const;
        DataFrame slice(DateTimeSlice, std::vector<std::string> const &columns) const;

        [[nodiscard]] DataFrame where(Series const &) const final;
        [[nodiscard]] DataFrame take(Series const &) const final;
        //</editor-fold>

        //<editor-fold desc="Iterator Functions">
        template<class OutType, class InType>
        DataFrame map(std::function<OutType(InType &&)> const& fn) const {
            pd::ArrayTable result;
            for (auto const &column: columnNames()) {
                result[column] = operator[](column).map(fn).array();
            }
            return DataFrame{result, m_index};
        }

        template<class InType>
        Series filter(std::function<bool(InType const&)> const& fn) const{
            return where(map<bool, InType>(fn));
        }
        //</editor-fold>

        std::shared_ptr<arrow::ChunkedArray> GetChunkedArray() const {
            return std::make_shared<arrow::ChunkedArray>(m_array->columns());
        }

        inline auto shape() const {
            return (m_array == nullptr) ? std::array<int64_t, 2>{0, 0} : std::array<int64_t, 2>{num_rows(),
                                                                                                num_columns()};
        }

        inline ArrayPtr fieldArray() const {
            return arrow::ArrayT<std::string>::Make(columnNames());
        }

        TablePtr toTable(std::optional<std::string> const &index_name = {}) const;

        template<class T>
        static std::pair<arrow::FieldVector, arrow::ArrayVector> makeFieldArrayPair(
                std::vector<std::vector<T>> const &table,
                std::vector<std::string> const &columns);

        template<template<typename...> class MapType, typename V>
        static std::pair<arrow::FieldVector, arrow::ArrayVector>
        makeFieldArrayPair(MapType<std::string, V> const &table);

        template<
                size_t N,
                size_t I = 0,
                class... PrimitiveTypes,
                typename TupleT = std::tuple<PrimitiveTypes...>,
                typename TupleVectorT = std::tuple<std::vector<PrimitiveTypes>...>>
        static void makeFieldArrayPairT(
                TupleVectorT &&columnData,
                arrow::ArrayVector &arrays,
                arrow::FieldVector &fields,
                std::array<std::string, N> const &columns);

        template<
                size_t N,
                size_t I = 0,
                class... PrimitiveTypes,
                typename TupleT = std::tuple<PrimitiveTypes...>,
                typename TupleVectorT = std::tuple<std::pair<std::string, std::vector<PrimitiveTypes>>...>>
        static void
        makeFieldArrayPairT(TupleVectorT &&columnData, arrow::ArrayVector &arrays, arrow::FieldVector &fields);

        DataFrame transpose() const;

        DataFrame &rename(std::unordered_map<std::string, std::string> const &columns);

        bool contains(std::string const &column) const;

        void add_prefix(std::string const &prefix, std::set<std::string> const &excludes = {});

        void add_suffix(std::string const &suffix, std::set<std::string> const &excludes = {});

        static std::vector<std::string> makeDefaultColumnNames(size_t N);

        inline int64_t num_rows() const {
            return m_array->num_rows();
        }

        inline int64_t num_columns() const {
            return m_array->num_columns();
        }

        template<size_t N>
        std::array<std::string, N> makeDefaultColumnNames();

        arrow::FieldVector dtypes() const;

        std::vector<std::string> columns() const;

        friend std::ostream &operator<<(std::ostream &a, DataFrame const &b);

        DataFrame describe(bool include_all = true, bool percentiles = false);

        static DataFrame readParquet(std::filesystem::path const &path);

        static DataFrame readCSV(std::filesystem::path const &path);

        static DataFrame
        readBinary(const std::basic_string_view<uint8_t> &blob, std::optional<std::string> const &index = std::nullopt);

        static DataFrame readJSON(const rapidjson::Document &doc, std::shared_ptr<arrow::Schema> const &schema,
                                  std::optional<std::string> const &index = std::nullopt);

        static DataFrame readJSON(const std::string &doc, std::shared_ptr<arrow::Schema> const &schema,
                                  std::optional<std::string> const &index = std::nullopt);

        arrow::Status toParquet(std::filesystem::path const &filepath, const std::string &indexField = "index") const;

        arrow::Status toCSV(std::filesystem::path const &filepath, const std::string &indexField = "index") const;

        arrow::Result<rapidjson::Value> toJSON(rapidjson::Document::AllocatorType &allocator,
                                               std::vector<std::string> columns = {},
                                               std::optional<std::string> const &index = {},
                                               bool toRecords = true) const;

        arrow::Result<std::shared_ptr<arrow::Buffer>>
        toBinary(std::vector<std::string> columns = {}, std::optional<std::string> const &index = {},
                 std::unordered_map<std::string, std::string> const &metadata = {}) const;

        pd::DataFrame reset_index(std::string const &columnName, bool drop) const;

        pd::DataFrame add_column(std::string const &newColumn, pd::ArrayPtr const &ptr) const;

        void add_column(std::string const &newColumn, pd::ArrayPtr const &ptr);

        void add_column(std::string const &newColumn, pd::Series const &ptr);

        pd::DataFrame reindex(std::shared_ptr<arrow::Array> const &newIndex,
                              const std::optional<Scalar> &fillValue = std::nullopt) const noexcept;

        pd::DataFrame reindexAsync(std::shared_ptr<arrow::Array> const &newIndex,
                                   const std::optional<Scalar> &fillValue = std::nullopt) const noexcept;

        Scalar at(std::shared_ptr<arrow::Scalar> const &row, std::string const &col) const {
            return at(
                    arrow::compute::Index(m_index,
                                          arrow::compute::IndexOptions(row))->scalar_as<arrow::Int64Scalar>().value,
                    col);
        }

        template<typename T>
        requires(not std::integral<T>)
        Scalar at(T const &row, std::string const &col)
        const {
            return at(arrow::MakeScalar(row), col);
        }

        DataFrame ffill() const;

        DataFrame bfill() const;

        [[nodiscard]] DataFrame is_null() const;

        [[nodiscard]] DataFrame is_valid() const;

        [[nodiscard]] DataFrame is_finite() const;

        [[nodiscard]] DataFrame is_infinite() const;

        void drop(std::vector<std::string> const &);

        inline void drop(std::string const &column) {
            drop(std::vector{column});
        }

        DataFrame drop_na() const;

        [[nodiscard]] Series index() const;

        [[nodiscard]] DataFrame head(int length = 5) const;

        [[nodiscard]] DataFrame tail(int length = 5) const;

        using NDFrame<arrow::RecordBatch>::setIndex;
        DataFrame setIndex(std::string const &column_name);

        DataFrame indexAsDateTime() const;

        DataFrame setColumns(std::vector<std::string> const &column_names);

        bool equals_(NDFrame<arrow::RecordBatch> const &other) const override {
            return m_array->Equals(*other.m_array) && m_index->Equals(other.indexArray());
        }

        template<typename T>
        requires(not std::same_as<DataFrame, T>)
        bool equals(std::vector<T> const &a) const;

        template<typename T>
        requires(not std::same_as<DataFrame, T>)
        bool approx_equals(std::vector<T> const &a) const;

        friend DataFrame operator*(Series const &s, DataFrame const &df);

        friend DataFrame operator/(Series const &s, DataFrame const &df);

        friend DataFrame operator+(Series const &s, DataFrame const &df);

        friend DataFrame operator-(Series const &s, DataFrame const &df);

        DataFrame operator|(DataFrame const &s) const;

        friend DataFrame operator|(Series const &s, DataFrame const &df);

        DataFrame operator&(DataFrame const &s) const;

        friend DataFrame operator&(Series const &s, DataFrame const &df);

        DataFrame operator^(DataFrame const &s) const;

        friend DataFrame operator^(Series const &s, DataFrame const &df);

        DataFrame operator<<(DataFrame const &s) const;

        friend DataFrame operator<<(Series const &s, DataFrame const &df);

        DataFrame operator>>(DataFrame const &s) const;

        friend DataFrame operator>>(Series const &s, DataFrame const &df);

        DataFrame operator>(DataFrame const &s) const;

        friend DataFrame operator>(Series const &s, DataFrame const &df);

        DataFrame operator>=(DataFrame const &s) const;

        friend DataFrame operator>=(Series const &s, DataFrame const &df);

        DataFrame operator<(DataFrame const &s) const;

        friend DataFrame operator<(Series const &s, DataFrame const &df);

        DataFrame operator<=(DataFrame const &s) const;

        friend DataFrame operator<=(Series const &s, DataFrame const &df);

        DataFrame operator==(DataFrame const &s) const;

        friend DataFrame operator==(Series const &s, DataFrame const &df);

        DataFrame operator!=(DataFrame const &s) const;

        friend DataFrame operator!=(Series const &s, DataFrame const &df);

        DataFrame operator&&(DataFrame const &s) const;

        friend DataFrame operator&&(Series const &s, DataFrame const &df);

        DataFrame operator||(DataFrame const &s) const;

        friend DataFrame operator||(Series const &s, DataFrame const &df);

        DataFrame unary(std::string const &functionName) const;

        DataFrame operator-() const {
            return unary("negate");
        }

        DataFrame operator~() const {
            return unary("bit_wise_not");
        }

        DataFrame operator==(Scalar const &a) const;

        DataFrame operator==(Series const &a) const;

        DataFrame operator!=(Scalar const &a) const;

        DataFrame operator!=(Series const &a) const;

        DataFrame operator<=(Scalar const &a) const;

        DataFrame operator<=(Series const &a) const;

        DataFrame operator>=(Scalar const &a) const;

        DataFrame operator>=(Series const &a) const;

        DataFrame operator>(Scalar const &a) const;

        DataFrame operator>(Series const &a) const;

        DataFrame operator<(Scalar const &a) const;

        DataFrame operator<(Series const &a) const;

        DataFrame operator|(Scalar const &a) const;

        DataFrame operator|(Series const &a) const;

        DataFrame operator&(Scalar const &a) const;

        DataFrame operator&(Series const &a) const;

        DataFrame operator||(Scalar const &a) const;

        DataFrame operator||(Series const &a) const;

        DataFrame operator&&(Scalar const &a) const;

        DataFrame operator&&(Series const &a) const;

        DataFrame operator^(Scalar const &a) const;

        DataFrame operator^(Series const &a) const;

        DataFrame operator<<(Scalar const &s) const;

        DataFrame operator<<(Series const &a) const;

        DataFrame operator>>(Scalar const &s) const;

        DataFrame operator>>(Series const &a) const;

        friend DataFrame operator+(Scalar const &a, DataFrame const &b);

        friend DataFrame operator/(Scalar const &a, DataFrame const &b);

        friend DataFrame operator*(Scalar const &a, DataFrame const &b);

        friend DataFrame operator-(Scalar const &a, DataFrame const &b);

        friend DataFrame operator|(Scalar const &a, DataFrame const &b);

        friend DataFrame operator&(Scalar const &a, DataFrame const &b);

        friend DataFrame operator^(Scalar const &a, DataFrame const &b);

        friend DataFrame operator||(Scalar const &a, DataFrame const &b);

        friend DataFrame operator&&(Scalar const &a, DataFrame const &b);

        friend DataFrame operator<<(Scalar const &a, DataFrame const &b);

        friend DataFrame operator>>(Scalar const &a, DataFrame const &b);

        friend DataFrame operator<(Scalar const &a, DataFrame const &b);

        friend DataFrame operator<=(Scalar const &a, DataFrame const &b);

        friend DataFrame operator>(Scalar const &a, DataFrame const &b);

        friend DataFrame operator>=(Scalar const &a, DataFrame const &b);

        friend DataFrame operator==(Scalar const &a, DataFrame const &b);

        friend DataFrame operator!=(Scalar const &a, DataFrame const &b);

        [[nodiscard]] Series argsort(std::vector<std::string> const &fields, bool ascending) const;

        [[nodiscard]] DataFrame sort_index(bool ascending = true, bool ignore_index = false);

        [[nodiscard]] DataFrame sort_values(
                std::vector<std::string> const &by,
                bool ascending = true,
                bool ignore_index = false);

        [[nodiscard]] class GroupBy group_by(std::string const &) const;

        [[nodiscard]] class Resampler resample(
                std::string const &rule,
                bool closed_right = false,
                bool label_right = false,
                TimeGrouperOrigin const &origin = {},
                time_duration const &offset = time_duration(),
                std::string const &tz = "") const;

        class Resampler downsample(std::string const &rule,
                                   bool closed_label_right = true,
                                   bool weekStartsMonday = true,
                                   bool startEpoch = true) const;

        [[nodiscard]] Series coalesce();

        [[nodiscard]] Series coalesce(std::vector<std::string> const &columns);

        std::vector<std::string> columnNames() const;

        Series mean(AxisType axis) const;

        DataFrame slice(int offset, int64_t length) const;

        template<bool inReverse, typename Visitor, typename Indices, typename... Spans>
        static void hmVisitInternal(Visitor &&visitor, Indices &&indices, Spans &&... spans) {
            static_assert(sizeof...(spans) <= 5, "Maximum of 5 spans are supported.");

            if constexpr (!inReverse) {
                // Forward iteration
                if constexpr (sizeof...(spans) == 1) {
                    auto [span1] = std::make_tuple(spans...);
                    visitor(indices.begin(), indices.end(), span1.begin(), span1.end());
                } else if constexpr (sizeof...(spans) == 2) {
                    auto [span1, span2] = std::make_tuple(spans...);
                    visitor(indices.begin(), indices.end(),
                            span1.begin(), span1.end(),
                            span2.begin(), span2.end());
                } else if constexpr (sizeof...(spans) == 3) {
                    auto [span1, span2, span3] = std::make_tuple(spans...);
                    visitor(indices.begin(), indices.end(),
                            span1.begin(), span1.end(),
                            span2.begin(), span2.end(),
                            span3.begin(), span3.end());
                } else if constexpr (sizeof...(spans) == 4) {
                    auto [span1, span2, span3, span4] = std::make_tuple(spans...);
                    visitor(indices.begin(), indices.end(),
                            span1.begin(), span1.end(),
                            span2.begin(), span2.end(),
                            span3.begin(), span3.end(),
                            span4.begin(), span4.end());
                } else if constexpr (sizeof...(spans) == 5) {
                    auto [span1, span2, span3, span4, span5] = std::make_tuple(spans...);
                    visitor(indices.begin(), indices.end(),
                            span1.begin(), span1.end(),
                            span2.begin(), span2.end(),
                            span3.begin(), span3.end(),
                            span4.begin(), span4.end(),
                            span5.begin(), span5.end());
                }
            } else {
                // Reverse iteration
                if constexpr (sizeof...(spans) == 1) {
                    auto [span1] = std::make_tuple(spans...);
                    visitor(indices.rbegin(), indices.rend(), span1.rbegin(), span1.rend());
                } else if constexpr (sizeof...(spans) == 2) {
                    auto [span1, span2] = std::make_tuple(spans...);
                    visitor(indices.rbegin(), indices.rend(),
                            span1.rbegin(), span1.rend(),
                            span2.rbegin(), span2.rend());
                } else if constexpr (sizeof...(spans) == 3) {
                    auto [span1, span2, span3] = std::make_tuple(spans...);
                    visitor(indices.rbegin(), indices.rend(),
                            span1.rbegin(), span1.rend(),
                            span2.rbegin(), span2.rend(),
                            span3.rbegin(), span3.rend());
                } else if constexpr (sizeof...(spans) == 4) {
                    auto [span1, span2, span3, span4] = std::make_tuple(spans...);
                    visitor(indices.rbegin(), indices.rend(),
                            span1.rbegin(), span1.rend(),
                            span2.rbegin(), span2.rend(),
                            span3.rbegin(), span3.rend(),
                            span4.rbegin(), span4.rend());
                } else if constexpr (sizeof...(spans) == 5) {
                    auto [span1, span2, span3, span4, span5] = std::make_tuple(spans...);
                    visitor(indices.rbegin(), indices.rend(),
                            span1.rbegin(), span1.rend(),
                            span2.rbegin(), span2.rend(),
                            span3.rbegin(), span3.rend(),
                            span4.rbegin(), span4.rend(),
                            span5.rbegin(), span5.rend());
                }
            }
        }

        template<typename V,
                bool in_reverse = false,
                typename ColumnType=double,
                typename ...NameType>
        V hmVisit(V &&visitor, NameType &&... names) const {
            if ((m_index->type()->id() != arrow::Type::INT64 && m_index->type()->id() != arrow::Type::TIMESTAMP)) {
                throw std::runtime_error("invalid index for hwdf Visitor only int64 are allowed.");
            }

            // Retrieve the expected Arrow type for the ColumnType
            auto expected_type = arrow::CTypeTraits<ColumnType>::type_singleton();
            for (const auto &name: {std::forward<NameType>(names)...}) {
                // Retrieve column by name
                auto column = m_array->GetColumnByName(name);
                if (!column) {
                    throw std::runtime_error(
                            fmt::format("Column '{}' does not exist.", name));
                }

                // Check if the column type matches the expected type
                if (column->type_id() != expected_type->id()) {
                    throw std::runtime_error(
                            fmt::format("Column '{}' has incompatible type: expected {}, but got {}",
                                        name, expected_type->ToString(), column->type()->ToString()));
                }
            }


            auto indices = getIndexSpan<int64_t>();

            visitor.pre();
            hmVisitInternal<!in_reverse>(visitor, indices, ((*this)[names].template getSpan<ColumnType>())...);
            visitor.post();

            return std::forward<V>(visitor);
        }

        template<typename ReturnT, typename FunctionSignature>
        Series rolling(FunctionSignature &&fn, int64_t window) const {
            return rollingT<false, ReturnT, Series, DataFrame>(std::forward<FunctionSignature>(fn), window, m_array, m_index);
        }

        template<typename ReturnT, typename FunctionSignature>
        Series expandRolling(FunctionSignature &&fn, int64_t minWindow) const {
            return rollingT<true, ReturnT, pd::Series, DataFrame>(std::forward<FunctionSignature>(fn), minWindow,
                                                       m_array, m_index);
        }

        template<class MapType>
        size_t GetTableRowSize(MapType const &table);
    };

    template<class... ColumnTypes, size_t N>
    DataFrame::DataFrame(
            std::array<std::string, N> columns,
            std::shared_ptr<arrow::Array> const &_index,
            std::vector<ColumnTypes> &&... columnData)
            : NDFrame<arrow::RecordBatch>(std::get<0>(std::forward_as_tuple(columnData...)).size(), _index) {

        if (m_index->length() == 0) {
            throw std::runtime_error("Cannot Create DataFrame with empty columns");
        }

        auto &&tuple = std::forward_as_tuple(columnData...);
        int64_t nRows = std::get<0>(tuple).size();

        if (columns.empty()) {
            columns = makeDefaultColumnNames<N>();
        }

        arrow::ArrayVector arrays(N);
        arrow::FieldVector fields(N);

        makeFieldArrayPairT<N, 0, ColumnTypes...>(std::forward_as_tuple(columnData...), arrays, fields,
                                                  std::move(columns));

        m_array = arrow::RecordBatch::Make(arrow::schema(fields), nRows, std::move(arrays));
    }

    template<class... ColumnTypes, size_t N>
    DataFrame::DataFrame(
            std::shared_ptr<arrow::Array> const &_index,
            Row<ColumnTypes> &&... columnData)
            : NDFrame<arrow::RecordBatch>(std::get<0>(std::forward_as_tuple(columnData...)).second.size(), _index) {
        static_assert(sizeof...(columnData) > 0, "Cannot Create DataFrame with empty columns");

        int64_t nRows = m_index->length();

        arrow::ArrayVector arrays(N);
        arrow::FieldVector fields(N);

        makeFieldArrayPairT<N, 0, ColumnTypes...>(std::forward_as_tuple(columnData...), arrays, fields);

        m_array = arrow::RecordBatch::Make(arrow::schema(fields), nRows, std::move(arrays));
    }

    template<class T>
    DataFrame::DataFrame(
            std::vector<std::vector<T>> const &table,
            std::vector<std::string> columns,
            std::shared_ptr<arrow::Array> const &_index)
            : NDFrame<arrow::RecordBatch>(table.back().size(), _index) {

        if (columns.empty()) {
            throw std::runtime_error("Cannot Create DataFrame with empty columns");
        }

        int nRows = table.back().size();

        if (columns.empty()) {
            columns = makeDefaultColumnNames(table.size());
        }

        auto [fields, arrays] = makeFieldArrayPair(table, std::move(columns));

        m_array = arrow::RecordBatch::Make(arrow::schema(fields), nRows, std::move(arrays));
    }

    template<template<typename...> class MapType, typename V>
    DataFrame::DataFrame(MapType<std::string, V> const &table, std::shared_ptr<arrow::Array> const &index)
            : NDFrame<arrow::RecordBatch>(GetTableRowSize(table), index) {
        if (table.empty()) {
            throw std::runtime_error("Cannot Create DataFrame with empty columns");
        }

        int64_t nRows = m_index->length();

        auto [fields, arrays] = makeFieldArrayPair(table);

        m_array = arrow::RecordBatch::Make(arrow::schema(fields), nRows, std::move(arrays));
    }

    template<class T>
    requires std::is_scalar_v<T> T getDefault(T scalarV) {
        return scalarV;
    }

    template<class T>
    T getDefault(std::vector<T> const &) {
        return T{};
    }

    template<class T>
    std::pair<arrow::FieldVector, arrow::ArrayVector> DataFrame::makeFieldArrayPair(
            std::vector<std::vector<T>> const &table,
            std::vector<std::string> const &columns) {
        arrow::ArrayVector arrays(table.size());
        arrow::FieldVector fields;
        fields.reserve(table.size());

        std::transform(
                table.begin(),
                table.end(),
                columns.begin(),
                arrays.begin(),
                [&fields](std::vector<T> const &column, std::string const &name) {
                    typename arrow::CTypeTraits<T>::BuilderType builder;
                    arrow::Status status;
                    if constexpr (std::is_floating_point_v<T>) {
                        status = builder.AppendValues(column, makeValidFlags(column));
                    } else {
                        status = builder.AppendValues(column);
                    }

                    if (status.ok()) {
                        auto result = builder.Finish();
                        if (result.ok()) {
                            fields.push_back(arrow::field(name, arrow::CTypeTraits<T>::type_singleton()));
                            return result.MoveValueUnsafe();
                        }
                        throw std::runtime_error(result.status().ToString());
                    }
                    throw std::runtime_error(status.ToString());
                });

        return {fields, arrays};
    }

    template<template<typename...> class MapType, typename V>
    std::pair<arrow::FieldVector, arrow::ArrayVector>
    DataFrame::makeFieldArrayPair(MapType<std::string, V> const &table) {
        constexpr bool isScalar = std::is_scalar_v<V>;
        using T = decltype(getDefault(V{}));

        arrow::ArrayVector arrays(table.size());
        arrow::FieldVector fields;
        fields.reserve(table.size());

        std::transform(
                table.begin(),
                table.end(),
                arrays.begin(),
                [&fields](std::pair<std::string, V> const &columnItem) {
                    typename arrow::CTypeTraits<T>::BuilderType builder;

                    auto [name, column] = columnItem;

                    arrow::Status status;
                    if constexpr (isScalar) {
                        status = isValid(column) ? builder.Append(column) : builder.AppendNull();
                    } else {
                        if constexpr (std::is_floating_point_v<T>) {
                            status = builder.AppendValues(column, makeValidFlags(column));
                        } else {
                            status = builder.AppendValues(column);
                        }
                    }

                    if (status.ok()) {
                        auto result = builder.Finish();
                        if (result.ok()) {
                            fields.push_back(arrow::field(name, arrow::CTypeTraits<T>::type_singleton()));
                            return result.MoveValueUnsafe();
                        }
                        throw std::runtime_error(result.status().ToString());
                    }
                    throw std::runtime_error(status.ToString());
                });

        return {fields, arrays};
    }

    template<size_t N, size_t I, class... PrimitiveTypes, typename TupleT, typename TupleVectorT>
    void DataFrame::makeFieldArrayPairT(
            TupleVectorT &&columnData,
            arrow::ArrayVector &arrays,
            arrow::FieldVector &fields,
            std::array<std::string, N> const &columns) {
        if constexpr (I < N) {
            using T = std::tuple_element_t<I, TupleT>;
            using Type = arrow::CTypeTraits<T>;

            typename Type::BuilderType builder;
            auto column = std::get<I>(columnData);

            if constexpr (std::is_floating_point_v<T>) {
                ThrowOnFailure(builder.AppendValues(column, makeValidFlags(column)));
            } else {
                ThrowOnFailure(builder.AppendValues(column));
            }

            arrays[I] = ReturnOrThrowOnFailure(builder.Finish());

            fields[I] = arrow::field(columns[I], Type::type_singleton());
            makeFieldArrayPairT < N, I + 1, PrimitiveTypes...>(
                    std::forward<TupleVectorT>(columnData),
                            arrays,
                            fields,
                            columns);
        }
    }

    template<class MapType>
    size_t DataFrame::GetTableRowSize(MapType const &table) {
        using T = typename MapType::mapped_type;
        if constexpr (std::is_scalar_v<T>) {
            return 1;
        } else {
            for (auto const &[_, value]: table) {
                if constexpr (std::is_same_v<T, ArrayPtr>) {
                    return not value ? 0 : value->length();
                } else {
                    return value.empty() ? 0 : value.size();
                }
            }
        }
        return 0;
    }

    template<size_t N, size_t I, class... PrimitiveTypes, typename TupleT, typename TupleVectorT>
    void
    DataFrame::makeFieldArrayPairT(TupleVectorT &&columnData, arrow::ArrayVector &arrays, arrow::FieldVector &fields) {
        if constexpr (I < N) {
            using T = std::tuple_element_t<I, TupleT>;
            using Type = arrow::CTypeTraits<T>;

            typename Type::BuilderType builder;
            auto [name, column] = std::get<I>(columnData);

            arrow::Status status;
            if constexpr (std::is_floating_point_v<T>) {
                ThrowOnFailure(builder.AppendValues(column, makeValidFlags(column)));
            } else {
                ThrowOnFailure(builder.AppendValues(column));
            }

            arrays[I] = ReturnOrThrowOnFailure(builder.Finish());

            fields[I] = arrow::field(name, Type::type_singleton());
            makeFieldArrayPairT < N, I + 1, PrimitiveTypes...>(std::forward<TupleVectorT>(columnData), arrays, fields);
        }
    }

    template<size_t N>
    std::array<std::string, N> DataFrame::makeDefaultColumnNames() {
        std::array<std::string, N> columns;
        for (size_t i = 0; i < N; i++) {
            columns[i] = std::to_string(i);
        }
        return columns;
    }

    template<typename T>
    requires(not std::same_as<DataFrame, T>) bool DataFrame::equals(std::vector<T> const &a) const {
        return NDFrame<arrow::RecordBatch>::equals_(DataFrame(a));
    }

    template<typename T>
    requires(not std::same_as<DataFrame, T>) bool DataFrame::approx_equals(std::vector<T> const &a) const {
        return NDFrame<arrow::RecordBatch>::approx_equals_(DataFrame(a));
    }
} // namespace pd