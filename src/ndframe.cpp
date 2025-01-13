//
// Created by adesola on 1/10/25.
//
#include "ndframe.h"
#include "dataframe.h"
#include "series.h"


#define GenericFunction(name, ReturnFilter, OutT) \
    template<class ArrayTypeImpl>                                                       \
    OutT NDFrame<ArrayTypeImpl>::name() const \
    { \
        auto result = arrow::compute::CallFunction(#name, { GetInternalArray() }); \
        if (result.ok()) \
        { \
            return OutT(ReturnFilter); \
        } \
        else \
        { \
            throw std::runtime_error(result.status().ToString()); \
        } \
    }

#define GenericFunctionScalarReturn(name) GenericFunction(name, result->scalar()->shared_from_this(), Scalar)

#define Aggregation(name) \
template<class ArrayTypeImpl>                          \
pd::Scalar NDFrame<ArrayTypeImpl>::name(bool skip_null) const { \
    arrow::compute::ScalarAggregateOptions opt{ skip_null }; \
    return ReturnScalarOrThrowOnError(arrow::compute::CallFunction(#name, { GetInternalArray() }, &opt)); \
}

#define AggregationT(name, T) \
    template<class ArrayTypeImpl>                          \
    T NDFrame<ArrayTypeImpl>::name(bool skip_null) const \
    { \
        arrow::compute::ScalarAggregateOptions opt{ skip_null }; \
        return ReturnScalarOrThrowOnError(arrow::compute::CallFunction(#name, { GetInternalArray() }, &opt)). template as<T>(); \
    }

#define AggregationWithCustomOption(name, f_name, ReturnT, option) \
    template<class ArrayTypeImpl>                                                               \
    ReturnT NDFrame<ArrayTypeImpl>::name() const \
    { \
        auto opt = option; \
        auto result = arrow::compute::CallFunction(#f_name, { GetInternalArray() }, &opt); \
        if (result.ok()) \
        { \
            return result->template scalar_as<typename arrow::CTypeTraits<ReturnT>::ScalarType>().value; \
        } \
        else \
        { \
            throw std::runtime_error(result.status().ToString()); \
        } \
    }

namespace pd {
    template<class ArrayTypeImpl>
    NDFrame<ArrayTypeImpl>::NDFrame(std::shared_ptr<arrow::Array> const &_index) : m_array(nullptr), m_index(_index) {
    }

    template<class ArrayTypeImpl>
    NDFrame<ArrayTypeImpl>::NDFrame(int64_t num_rows) : m_array(nullptr), m_index(uint_range(num_rows)) {
    }

    template<class ArrayTypeImpl>
    NDFrame<ArrayTypeImpl>::NDFrame(int64_t num_rows, std::shared_ptr<arrow::Array> const &_index) : m_array(nullptr) {
        if (_index) {
            m_index = _index;
        } else {
            m_index = uint_range(num_rows);
        }

        if (num_rows != m_index->length()) {
            throw std::runtime_error(
                    fmt::format("NDFrame: Number of rows({}) does not match array length({})", num_rows,
                                m_index->length()));
        }
    }

    template<class ArrayTypeImpl>
    NDFrame<ArrayTypeImpl>::NDFrame(ArrayType const &array, int64_t num_rows) : m_array(array),
                                                                                m_index(uint_range(num_rows)) {
    }

    template<class ArrayTypeImpl>
    NDFrame<ArrayTypeImpl>::NDFrame(ArrayType const &array, std::shared_ptr<arrow::Array> const &_index, bool skipIndex)
            : m_array(array), m_index(_index) {
        if (not m_index and not skipIndex) {
            if (m_array) {
                if constexpr (std::same_as<ArrayType, std::shared_ptr<arrow::RecordBatch>>) {
                    m_index = uint_range(array->num_rows());
                } else {
                    m_index = uint_range(array->length());
                }
            }
        }
    }

    std::shared_ptr<arrow::Array> uint_range(int64_t n_rows) {
        std::vector<uint64_t> index(n_rows);
        std::iota(index.begin(), index.end(), 0);
        arrow::UInt64Builder builder;

        ThrowOnFailure(builder.AppendValues(index));
        return ReturnOrThrowOnFailure(builder.Finish());
    }

    //<editor-fold desc="Aggregation Functions">
    AggregationT(all, bool)

    AggregationT(any, bool)

    template<class ArrayTypeImpl>
    Scalar NDFrame<ArrayTypeImpl>::median(bool skip_null) const {
        return agg("approximate_median", skip_null);
    }

    AggregationWithCustomOption(count, count, int64_t, arrow::compute::CountOptions{})

    AggregationWithCustomOption(
            count_na,
            count,
            int64_t,
            arrow::compute::CountOptions{arrow::compute::CountOptions::CountMode::ONLY_NULL})

    AggregationWithCustomOption(nunique, count_distinct, int64_t, arrow::compute::CountOptions{})

    Aggregation(first)

    template<class ArrayTypeImpl>
    int64_t NDFrame<ArrayTypeImpl>::index(Scalar const &search) const {
        if (isIndex) {
            try {
                return indexer.at(search.scalar);
            }
            catch (std::out_of_range const &) {
                return -1;
            }
        } else {
            auto result = arrow::compute::Index(GetInternalArray(), arrow::compute::IndexOptions{search.value()});
            if (result.ok()) {
                return result->template scalar_as<arrow::Int64Scalar>().value;
            } else {
                throw std::runtime_error(result.status().ToString());
            }
        }
    }

    template<class ArrayTypeImpl>
    std::array<Scalar, 2> NDFrame<ArrayTypeImpl>::first_last(bool skip_null) const {
        arrow::compute::ScalarAggregateOptions opt{skip_null};
        auto result =
                pd::ReturnOrThrowOnFailure(arrow::compute::CallFunction("first_last", {GetInternalArray()}, &opt));

        auto firstLast = result.template scalar_as<arrow::StructScalar>().value;
        return {pd::Scalar(std::move(firstLast[0])), pd::Scalar(std::move(firstLast[1]))};
    }

    Aggregation(last)

    Aggregation(max)

    Aggregation(mean)

    Aggregation(min)

    template<class ArrayTypeImpl>
    MinMax NDFrame<ArrayTypeImpl>::min_max(bool skip_null) const {
        auto result =
                ReturnOrThrowOnFailure(
                        arrow::compute::MinMax(GetInternalArray(), arrow::compute::ScalarAggregateOptions{skip_null}));
        auto minmax = result.template scalar_as<arrow::StructScalar>().value;
        return MinMax{pd::Scalar(std::move(minmax[0])), pd::Scalar(std::move(minmax[1]))};
    }

    template<class ArrayTypeImpl>
    std::vector<Mode> NDFrame<ArrayTypeImpl>::mode(int64_t n, bool skip_null, uint32_t minCount) const {
        arrow::Datum result =
                ReturnOrThrowOnFailure(
                        arrow::compute::Mode(GetInternalArray(), arrow::compute::ModeOptions{n, skip_null, minCount}));
        auto arr = result.array_as<arrow::StructArray>();

        auto count = arr->length();
        std::vector<Mode> modes(count);

        auto modeArr = arr->GetFieldByName("mode");
        auto countArr = arrow::checked_pointer_cast<arrow::Int64Array>(arr->GetFieldByName("count"));

        for (int64_t i: std::views::iota(0L, count)) {
            modes[i] = Mode{
                    pd::Scalar(modeArr->GetScalar(i)),
                    countArr->Value(i)
            };
        }
        return modes;
    }

    Aggregation(product)

    template<class ArrayTypeImpl>
    Scalar NDFrame<ArrayTypeImpl>::quantile(double q,
                                            arrow::compute::QuantileOptions::Interpolation interpolation,
                                            bool skip_nulls, uint32_t min_count) const {
        arrow::compute::QuantileOptions opt{q, interpolation, skip_nulls, min_count};
        auto result = arrow::compute::Quantile(GetInternalArray(), opt);
        if (result.ok()) {
            return Scalar(pd::ReturnOrThrowOnFailure(result->make_array()->GetScalar(0)));
        } else {
            throw std::runtime_error(result.status().ToString());
        }
    }

    template<class ArrayTypeImpl>
    Scalar NDFrame<ArrayTypeImpl>::std(int ddof, bool skip_na) const {
        return ReturnScalarOrThrowOnError(
                arrow::compute::Stddev(GetInternalArray(), arrow::compute::VarianceOptions{ddof, skip_na}));
    }

    Aggregation(sum)

    template<class ArrayTypeImpl>
    Scalar NDFrame<ArrayTypeImpl>::tdigest(double q, uint32_t delta,
                                           uint32_t buffer_size, bool skip_nulls,
                                           uint32_t min_count) const {
        arrow::compute::TDigestOptions opt{q, delta, buffer_size, skip_nulls, min_count};
        auto result = ReturnOrThrowOnFailure(arrow::compute::TDigest({GetInternalArray()}, opt)).make_array();
        return Scalar{ReturnOrThrowOnFailure(result->GetScalar(0))};
    }

    template<class ArrayTypeImpl>
    Scalar NDFrame<ArrayTypeImpl>::var(int ddof, bool skip_na) const {
        return ReturnScalarOrThrowOnError(
                arrow::compute::Variance(GetInternalArray(), arrow::compute::VarianceOptions{ddof, skip_na}));
    }

    template<class ArrayTypeImpl>
    Scalar NDFrame<ArrayTypeImpl>::agg(std::string const &name, bool skip_null) const {
        arrow::compute::ScalarAggregateOptions opt{skip_null};
        return pd::ReturnScalarOrThrowOnError(arrow::compute::CallFunction(name, {GetInternalArray()}, &opt));
    }
    //</editor-fold>

    //<editor-fold desc="Indexing Functions">
    template<class ArrayTypeImpl>
    NDFrame<ArrayTypeImpl>::ChildType NDFrame<ArrayTypeImpl>::operator[](DateTimeSlice const & slicer) const
    {
        if (m_index->type_id() == arrow::Type::TIMESTAMP) {
            int64_t start = 0, end = m_index->length();

            if (slicer.start) {
                start = ReturnScalarOrThrowOnError(
                        arrow::compute::Index(m_index,
                                              arrow::compute::IndexOptions{fromDateTime(slicer.start.value())})).as<int64_t>();
                if (start == -1) {
                    throw std::runtime_error("invalid start index");
                }
            }
            if (slicer.end) {
                end = ReturnScalarOrThrowOnError(
                        arrow::compute::Index(m_index, arrow::compute::IndexOptions{fromDateTime(slicer.end.value())})).as<int64_t>();
                if (end == -1) {
                    throw std::runtime_error("invalid end index");
                }
            }
            return operator[]({start, end});
        } else {
            std::stringstream ss;
            ss << "Type Error: DateTime slicing is only allowed on TimeStamp DataType, but found index of type "
               << m_index->type()->ToString() << "\n";
            throw std::runtime_error(ss.str());
        }
    }

    template<class ArrayTypeImpl>
    NDFrame<ArrayTypeImpl>::ChildType NDFrame<ArrayTypeImpl>::operator[](DateSlice const & s) const{
        return operator[]({s.start ? std::optional{ptime(*s.start)} : std::nullopt,
                           s.end  ? std::optional{ptime(*s.end)} : std::nullopt});
    }

    template<class ArrayTypeImpl>
    NDFrame<ArrayTypeImpl>::ChildType NDFrame<ArrayTypeImpl>::operator[](StringSlice const & slicer) const
    {
        if (m_index->type_id() == arrow::Type::STRING) {
            int64_t start = 0, end = m_index->length();
            if (slicer.start) {
                start = ReturnScalarOrThrowOnError(arrow::compute::Index(
                        m_index,
                        arrow::compute::IndexOptions{arrow::MakeScalar(slicer.start.value())})).as<int64_t>();
                if (start == -1) {
                    throw std::runtime_error("invalid start index");
                }
            }
            if (slicer.end) {
                end = ReturnScalarOrThrowOnError(
                        arrow::compute::Index(m_index,
                                              arrow::compute::IndexOptions{arrow::MakeScalar(slicer.end.value())})).as<int64_t>();
                if (end == -1) {
                    throw std::runtime_error("invalid end index");
                }
            }
            return operator[]({start, end});
        } else {
            std::stringstream ss;
            ss << "Type Error: String slicing is only allowed on STRING DataType, but found index of type "
               << m_index->type()->ToString() << "\n";
            throw std::runtime_error(ss.str());
        }
    }

    template<class ArrayTypeImpl>
    NDFrame<ArrayTypeImpl>::ChildType NDFrame<ArrayTypeImpl>::operator[](Series const &index) const {
        return index.dtype()->id() == arrow::Type::BOOL ? where(index) : take(index);
    }

    template<class ArrayTypeImpl>
    NDFrame<ArrayTypeImpl>::ChildType NDFrame<ArrayTypeImpl>::setIndex(std::shared_ptr<arrow::Array> const &index) const {
        return ChildType{
            m_array, index
        };
    }
    //</editor-fold>

    template<class ArrayTypeImpl>
    pd::ArrayPtr NDFrame<ArrayTypeImpl>::normalizeIndex() const {
        if (m_index->type_id() != arrow::Type::TIMESTAMP) {
            auto error = fmt::format("Index must be of type TIMESTAMP, but found index of type {}",
                                     m_index->type()->ToString());
            throw std::runtime_error(error);
        }

        return pd::ReturnOrThrowOnFailure(arrow::compute::FloorTemporal(m_index,
                                                                        arrow::compute::RoundTemporalOptions{1,
                                                                                                             arrow::compute::CalendarUnit::DAY})).make_array();

    }

    template
    class NDFrame<arrow::Array>;

    template
    class NDFrame<arrow::RecordBatch>;
}