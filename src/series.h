//
// Created by dewe on 12/28/22.
//
#pragma once

#include <algorithm>
#include <arrow/util/checked_cast.h>
#include <cmath>
#include <iostream>
#include <span>
#include <vector>
#include "ndframe.h"


namespace pd {

    using namespace std;

    class Series : public NDFrame<Series> {

    public:
        using ArrayType = std::shared_ptr<arrow::Array>;

        Series(std::shared_ptr<arrow::Array> const &arr, bool isIndex, std::string name = "");

        Series(
                std::shared_ptr<arrow::Array> const &arr,
                std::shared_ptr<arrow::Array> const &index,
                std::string name = "",
                bool skipIndex = false);

        template<typename T>
        Series(std::vector<T> const &arr, std::string name = "", std::shared_ptr<arrow::Array> const &index = nullptr)
                : Series(std::move(Make<T, typename arrow::CTypeTraits<T>::BuilderType>(arr, name, index))) {
        }

        Series(
                arrow::ScalarVector const &arr,
                std::shared_ptr<arrow::DataType> const &dataType,
                std::string const &name = "",
                std::shared_ptr<arrow::Array> const &index = nullptr)
                : NDFrame<Series>(index), m_name(name) {
            auto builder = pd::ReturnOrThrowOnFailure(arrow::MakeBuilder(dataType));
            ThrowOnFailure(builder->AppendScalars(arr));
            m_array = pd::ReturnOrThrowOnFailure(builder->Finish());
        }

        // Check if the Series object is an index array.
        inline bool IsIndexArray() const noexcept {
            return isIndex;
        }

        // Get the indexer for the Series object.
        inline auto getIndexer() const noexcept {
            return indexer;
        }

        template<typename T>
        requires std::is_scalar_v<T>
        static Series MakeScalar(
                T const &v,
                std::string const &name = "",
                std::shared_ptr<arrow::Array> const &index = nullptr) {
            return {std::vector<T>{v}, name, index};
        }

        template<typename T, typename BuilderT = typename arrow::CTypeTraits<T>::BuilderType>
        [[nodiscard]] static Series Make(
                std::vector<T> const &arr,
                std::string _name = "series",
                std::shared_ptr<arrow::Array> const &index = nullptr,
                bool skipIndex = false);

        template<typename T>
        auto const_ptr() const {
            return reinterpret_cast<const T *>(m_array->data()->buffers[1]->data()) + m_array->offset();
        }

        template<typename T>
        std::vector<T> values() const;

        template<typename T>
        std::shared_ptr<typename arrow::CTypeTraits<T>::ArrayType> view() const;

        // Basic Operation
        template<typename T>
        [[nodiscard]] bool is() const;

        [[nodiscard]] std::shared_ptr<arrow::DataType> dtype() const;

        [[nodiscard]] uint64_t nbytes() const;

        [[nodiscard]] int64_t size() const;

        [[nodiscard]] bool empty() const;

        [[nodiscard]] std::string name() const;

        void add_prefix(std::string prefix);

        inline auto value() const {
            return m_array;
        }

        template<typename T>
        std::span<const T> getSpan() const {
            static_assert(not std::is_same_v<T, bool>);
            return {this->const_ptr<T>(), size_t(size())};
        }

        using Shape = std::array<::int64_t, 1>;

        inline Shape shape() {
            return {size()};
        }

        void add_suffix(std::string const &suffix);

        void rename(std::string const &newName);

        friend std::ostream &operator<<(std::ostream &a, Series const &b);

        template<typename T>
        requires(not std::same_as<Series, T>) inline bool equals(std::vector<T> const &a) const;

        template<typename T>
        requires(not std::same_as<Series, T>) bool approx_equals(std::vector<T> const &a) const;

        // Indexing Operation
        Scalar operator[](int index) const;

        inline Scalar at(int index) const {
            return operator[](index);
        }

        Series operator[](Slice) const;

        Series operator[](DateTimeSlice const &) const;

        Series operator[](StringSlice const &) const;

        inline Series operator[](Series const &mask) const {
            return take(mask);
        }

        // Math Operations
        Series operator~() const;

        Series operator!() const;

        Series operator-() const;

        Series operator+(class Series const &s) const;

        Series operator+(class Scalar const &s) const;

        Series operator-(Series const &s) const;

        Series operator-(Scalar const &s) const;

        Series operator/(Series const &a) const;

        Series operator/(Scalar const &a) const;

        Series operator==(Scalar const &a) const;

        Series operator==(Series const &a) const;

        Series operator!=(Scalar const &a) const;

        Series operator!=(Series const &a) const;

        Series operator<=(Scalar const &a) const;

        Series operator<=(Series const &a) const;

        Series operator>=(Scalar const &a) const;

        Series operator>=(Series const &a) const;

        Series operator>(Scalar const &a) const;

        Series operator>(Series const &a) const;

        Series operator<(Scalar const &a) const;

        Series operator<(Series const &a) const;

        Series operator|(Scalar const &a) const;

        Series operator|(Series const &a) const;

        Series operator&(Scalar const &a) const;

        Series operator&(Series const &a) const;

        Series operator||(Scalar const &a) const;

        Series operator||(Series const &a) const;

        Series operator&&(Scalar const &a) const;

        Series operator&&(Series const &a) const;

        Series operator^(Scalar const &a) const;

        Series operator^(Series const &a) const;

        Series operator<<(Scalar const &s) const;

        Series operator<<(Series const &a) const;

        Series operator>>(Scalar const &s) const;

        Series operator>>(Series const &a) const;

        virtual Series operator*(Series const &a) const;

        virtual Series operator*(Scalar const &a) const;

        friend Series operator+(Scalar const &a, Series const &b);

        friend Series operator/(Scalar const &a, Series const &b);

        friend Series operator*(Scalar const &a, Series const &b);

        friend Series operator-(Scalar const &a, Series const &b);

        friend Series operator|(Scalar const &a, Series const &b);

        friend Series operator&(Scalar const &a, Series const &b);

        friend Series operator^(Scalar const &a, Series const &b);

        friend Series operator||(Scalar const &a, Series const &b);

        friend Series operator&&(Scalar const &a, Series const &b);

        friend Series operator<<(Scalar const &a, Series const &b);

        friend Series operator>>(Scalar const &a, Series const &b);

        friend Series operator<(Scalar const &a, Series const &b);

        friend Series operator<=(Scalar const &a, Series const &b);

        friend Series operator>(Scalar const &a, Series const &b);

        friend Series operator>=(Scalar const &a, Series const &b);

        friend Series operator==(Scalar const &a, Series const &b);

        friend Series operator!=(Scalar const &a, Series const &b);

        // Math Functions
        [[nodiscard]] Series abs() const;

        [[nodiscard]] Series pow(double x) const;

        [[nodiscard]] Series sqrt() const;

        [[nodiscard]] Series sign() const;

        [[nodiscard]] Series ln() const;

        [[nodiscard]] Series log10() const;

        [[nodiscard]] Series log1p() const;

        [[nodiscard]] Series log2() const;

        [[nodiscard]] Series logb(int base) const;

        [[nodiscard]] Series cos() const;

        [[nodiscard]] Series sin() const;

        [[nodiscard]] Series tan() const;

        [[nodiscard]] Series ceil() const;

        [[nodiscard]] Series floor() const;

        [[nodiscard]] Series round() const;

        [[nodiscard]] Series round_to_multiple(double multiple, arrow::compute::RoundMode roundMode) const;

        [[nodiscard]] Series trunc() const;

        [[nodiscard]] Series cumsum(double start = 0, bool skip_nulls = true) const;

        [[nodiscard]] Series cumprod(double start = 1, bool skip_nulls = true) const;

        [[nodiscard]] Series cummax(double start, bool skip_nulls = true) const;

        [[nodiscard]] Series cummin(double start, bool skip_nulls = true) const;

        // agg functions
        [[nodiscard]] MinMax min_max(bool skip_nulls) const;

        [[nodiscard]] Scalar agg(std::string const &func, bool skip_null = true) const;

        [[nodiscard]] Scalar min() const;

        [[nodiscard]] Scalar max() const;

        [[nodiscard]] Scalar product() const;

        [[nodiscard]] Scalar sum() const;

        [[nodiscard]] DataFrame mode(int n, bool skip_nulls) const;

        [[nodiscard]] Scalar quantile(double q = 0.5) const;

        [[nodiscard]] Scalar tdigest(double q = 0.5) const;

        [[nodiscard]] double median(bool skip_null = true) const;

        [[nodiscard]] double mean(bool skip_null = true) const;

        [[nodiscard]] double std(int ddof = 1, bool skip_na = true) const;

        [[nodiscard]] double var(int ddof = 1, bool skip_na = true) const;

        // utility functions
        [[nodiscard]] bool all(bool skip_null = true) const;

        [[nodiscard]] bool any(bool skip_null = true) const;

        [[nodiscard]] int64_t index(Scalar const &search) const;

        [[nodiscard]] int64_t count() const;

        [[nodiscard]] int64_t count_na() const;

        [[nodiscard]] int64_t nunique() const;

        [[nodiscard]] bool is_unique() const;

        [[nodiscard]] Series where(Series const &) const;

        [[nodiscard]] Series take(Series const &) const;

        [[nodiscard]] class DataFrame value_counts() const;

        [[nodiscard]] std::shared_ptr<arrow::DictionaryArray> dictionary_encode() const;

        pd::Series reindex(
                std::shared_ptr<arrow::Array> const &newIndex,
                std::optional<std::unordered_map<int64_t, int64_t>> indexer = std::nullopt,
                const std::optional<Scalar> &fillValue = std::nullopt) const;

        pd::Series reindexAsync(
                std::shared_ptr<arrow::Array> const &newIndex,
                std::optional<std::unordered_map<int64_t, int64_t>> indexer = std::nullopt,
                const std::optional<Scalar> &fillValue = std::nullopt) const;

        class Resampler resample(
                std::string const &rule,
                bool closed_right = false,
                bool label_right = false,
                TimeGrouperOrigin const &origin = {},
                time_duration const &offset = time_duration(),
                std::string const &tz = "") const;

        DataFrame toFrame(std::optional<std::string> const &name = {}) const;

        [[nodiscard]] Series unique() const;

        [[nodiscard]] Series drop_na() const;

        [[nodiscard]] Series indices_nonzero() const;

        [[nodiscard]] Series ffill() const;

        [[nodiscard]] Series bfill() const;

        [[nodiscard]] Series where(Series const &cond, Series const &other) const;

        [[nodiscard]] Series where(Series const &cond, Scalar const &other) const;

        [[nodiscard]] Series replace_with_mask(Series const &cond, Series const &other) const;

        [[nodiscard]] Series if_else(Series const &truth_values, Series const &other) const;

        [[nodiscard]] Series if_else(Series const &truth_values, Scalar const &other) const;

        bool is_valid(int row) const;

        [[nodiscard]] Series is_null() const;

        [[nodiscard]] Series is_valid() const;

        [[nodiscard]] Series is_finite() const;

        [[nodiscard]] Series is_infinite() const;

        [[nodiscard]] struct StringLike str() const;

        [[nodiscard]] struct DateTimeLike dt() const;

        [[nodiscard]] Series cast(std::shared_ptr<arrow::DataType> const &dt, bool safe = true) const;

        template<typename T>
        [[nodiscard]] inline Series cast(bool safe = true) const {
            return cast(arrow::CTypeTraits<T>::type_singleton(), safe);
        }

        [[nodiscard]] Series to_datetime() const;

        [[nodiscard]] Series to_datetime(std::string const &format) const;

        [[nodiscard]] Series shift(int32_t shift_value = 1, std::shared_ptr<arrow::Scalar> const &fill_value = nullptr)
        const;

        [[nodiscard]] Series pct_change(int64_t periods = 1) const;

        [[nodiscard]] Series append(Series const &to_append, bool ignore_index = false) const;

        template<typename DataT, typename OutputT = DataT, typename... Args>
        [[nodiscard]] Series apply(auto &&func, Args &&... args) const;

        Series intersection(Series const &other) const;

        Series union_(Series const &other) const;

        [[nodiscard]] int64_t argmax() const {
            return index(max());
        }

        [[nodiscard]] int64_t argmin() const {
            return index(min());
        }

        [[nodiscard]] Series argsort(bool ascending = true) const;

        [[nodiscard]] std::array<std::shared_ptr<arrow::Array>, 2> sort(bool ascending = true) const;

        [[nodiscard]] Series nth_element(int n = 0) const;

        [[nodiscard]] double corr(const Series &s2, CorrelationType method = CorrelationType::Pearson) const;

        [[nodiscard]] double corr(const Series &s2, double (*method)(double)) const;

        [[nodiscard]] double cov(Series const &S2) const;

        [[nodiscard]] Series ewm(
                EWMAgg agg,
                double value,
                EWMAlphaType type,
                bool adjust = true,
                bool ignore_na = false,
                int min_periods = 0,
                bool bias = false) const;

        [[nodiscard]] Series strftime(std::string const &format, std::string const &locale = "C") const;

        [[nodiscard]] Series strptime(std::string const &format, arrow::TimeUnit::type unit, bool error_is_null = false)
        const;

        Series ReturnSeriesOrThrowOnError(arrow::Result<arrow::Datum> &&result) const;

        bool approx_equals_(pd::Series const &a, double eps) const {
            return m_array->ApproxEquals(*a.m_array, arrow::EqualOptions::Defaults().atol(eps));
        }

        std::string diff_str(pd::Series const &a) const {
            return m_array->Diff(*a.m_array);
        }

        template<typename V, bool in_reverse = false, typename IndexType=int64_t, typename ColumnType=double>
        V hmVisit(V && v) const {
            auto visitor = std::forward<V>(v);
            auto indices = getIndexSpan<IndexType>();
            auto vec = getSpan<ColumnType>();

            if (m_array->null_count() != 0) {
                throw std::runtime_error("hmVisit currently only supports non-null array.");
            }

            visitor.pre();
            if constexpr (!in_reverse)
                visitor(indices.begin(), indices.end(), vec.begin(), vec.end());
            else
                visitor(indices.rbegin(), indices.rend(), vec.rbegin(), vec.rend());
            visitor.post();

            return v;
        }

        template<typename ReturnT, typename FunctionSignature>
        Series rolling(FunctionSignature && fn, int64_t window){
            return rollingT<ReturnT, pd::Series>(std::forward<FunctionSignature>(fn), window, m_array->length());
        }

    private:
        std::string m_name;

        std::vector<std::shared_ptr<arrow::Scalar>> to_vector() const;

        std::vector<std::shared_ptr<arrow::Scalar>> get_indexed_values() const;

        static vector<double> ewm(
                const std::shared_ptr<arrow::DoubleArray> &vals,
                const std::vector<int64_t> &start,
                const std::vector<int64_t> &end,
                int minp = 1,
                double com = 1,
                bool adjust = true,
                bool ignore_na = false,
                const vector<double> &deltas = {},
                bool normalize = true);

        static vector<double> ewmcov(
                const std::shared_ptr<arrow::DoubleArray> &input_x,
                const std::vector<int64_t> &start,
                const std::vector<int64_t> &end,
                const std::shared_ptr<arrow::DoubleArray> &input_y,
                int minp = 1,
                double com = 9.5,
                bool adjust = true,
                bool ignore_na = false,
                bool bias = false);
    };

// Template Implementation
    template<typename T>
    requires(not std::same_as<Series, T>) inline bool Series::equals(std::vector<T> const &a) const {
        return NDFrame<Series>::equals_(Series(a));
    }

    template<typename T>
    requires(not std::same_as<Series, T>) bool Series::approx_equals(std::vector<T> const &a) const {
        return NDFrame<Series>::approx_equals_(Series(a));
    }

    template<typename T, typename BuilderT>
    Series Series::Make(
            std::vector<T> const &arr,
            std::string _name,
            std::shared_ptr<arrow::Array> const &index,
            bool skipIndex) {
        BuilderT builder;
        std::vector<uint8_t> cacheForStringArrayNullBitMap;

        auto status = builder.Reserve(arr.size());
        if (status.ok()) {
            if constexpr (std::is_same_v<T, std::string>) {
                cacheForStringArrayNullBitMap = makeValidFlags(arr);
                status = builder.AppendValues(arr, cacheForStringArrayNullBitMap.data());
            } else {
                status = builder.AppendValues(arr, makeValidFlags(arr));
            }
        }

        if (status.ok()) {
            auto &&arrArrow = builder.Finish();
            if (arrArrow.ok()) {
                return {arrArrow.MoveValueUnsafe(), index, std::move(_name), skipIndex};
            } else {
                throw std::runtime_error(arrArrow.status().ToString());
            }
        } else {
            throw std::runtime_error(status.ToString());
        }
    }

    template<typename T>
    [[nodiscard]] bool Series::is() const {
        return m_array->type() == arrow::CTypeTraits<T>::type_singleton();
    }

    template<typename T>
    std::vector<T> Series::values() const {
        static_assert(not std::is_same_v<T, bool>);

        auto requested_type = arrow::CTypeTraits<T>::type_singleton();
        if (requested_type == m_array->type()) {
            std::vector<T> result;
            result.resize(m_array->length());

            if constexpr (std::same_as<T, std::string>) {
                auto stringSpan = getSpan<std::string_view>();
                std::ranges::copy(stringSpan, result.begin());
            } else {
                auto realArray = std::static_pointer_cast<typename arrow::CTypeTraits<T>::ArrayType>(m_array);

                auto N = realArray->length();
                auto ptr = realArray->raw_values();

                memcpy(result.data(), ptr, sizeof(T) * N);
            }
            return result;
        } else {
            throw RawArrayCastException(requested_type, m_array->type());
        }
    }

    template<typename T>
    std::shared_ptr<typename arrow::CTypeTraits<T>::ArrayType> Series::view() const {
        static_assert(not std::is_same_v<T, bool>);

        auto requested_type = arrow::CTypeTraits<T>::type_singleton();
        if (requested_type == m_array->type()) {
            return std::static_pointer_cast<typename arrow::CTypeTraits<T>::ArrayType>(m_array);
        } else {
            throw RawArrayCastException(requested_type, m_array->type());
        }
    }

    template<typename DataT, typename OutputT, typename... Args>
    Series Series::apply(auto &&func, Args &&... args) const {

        typename arrow::CTypeTraits<OutputT>::BuilderType builder;
        auto realArray = std::static_pointer_cast<typename arrow::CTypeTraits<DataT>::ArrayType>(m_array);

        if (realArray) {
            int64_t N = realArray->length();
            ThrowOnFailure(builder.Reserve(N));
            for (auto const &element: *realArray) {
                OutputT res = func(*element, std::forward<Args>(args)...);
                builder.UnsafeAppend(res);
            }
            return {ReturnOrThrowOnFailure(builder.Finish()), m_index, m_name};
        }
        throw RawArrayCastException{arrow::CTypeTraits<DataT>::type_singleton(), m_array->type()};
    }

} // namespace pd
