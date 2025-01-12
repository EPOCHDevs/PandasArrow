//
// Created by dewe on 12/28/22.
//
#include "series.h"
#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/io/api.h>
#include <tabulate/table.hpp>
#include <unordered_set>
#include "boost/format.hpp"
#include "datetimelike.h"
#include "filesystem"
#include "ranges"
#include "resample.h"
#include "stringlike.h"
#include <DataFrame/DataFrameFinancialVisitors.h>


#define BINARY_OPERATOR(sign, name) \
    Series Series::operator sign(const Series& a) const \
    { \
        return ReturnSeriesOrThrowOnError(arrow::compute::CallFunction(#name, { m_array, a.m_array })); \
    } \
\
    Series Series::operator sign(const Scalar& a) const \
    { \
        return ReturnSeriesOrThrowOnError(arrow::compute::CallFunction(#name, { m_array, a.scalar })); \
    } \
\
    Series operator sign(Scalar const& a, Series const& b) \
    { \
        return ReturnSeriesOrThrowOnError(arrow::compute::CallFunction(#name, { a.value(), b.m_array })); \
    }

#define GenericFunction(name, ReturnFilter, OutT, ClassT) \
    OutT ClassT::name() const \
    { \
        auto result = arrow::compute::CallFunction(#name, { m_array }); \
        if (result.ok()) \
        { \
            return OutT(ReturnFilter); \
        } \
        else \
        { \
            throw std::runtime_error(result.status().ToString()); \
        } \
    }

#define GenericFunctionSeriesReturnRename(name, f_name, ClassT) \
    Series ClassT ::name() const \
    { \
        return ReturnSeriesOrThrowOnError(arrow::compute::CallFunction(#f_name, { m_array })); \
    }

#define GenericFunctionSeriesReturn(name) \
    GenericFunction(name, (ReturnSeriesOrThrowOnError(std::move(result))), Series, Series)

#define GenericFunctionSeriesReturnString(name) \
    GenericFunction(name, (ReturnSeriesOrThrowOnError(std::move(result))), Series, StringLike)

#define GenericFunctionSeriesReturnDateTimeLike(name) \
    GenericFunction(name, (ReturnSeriesOrThrowOnError(std::move(result))), Series, DateTimeLike)

#define GenericFunctionScalarReturnString(name) \
    GenericFunction(name, result->scalar()->shared_from_this(), Scalar, StringLike)

#define GenericFunctionScalarReturnDateTimeLike(name) \
    GenericFunction(name, result->scalar()->shared_from_this(), Scalar, DateTimeLike)

namespace pd {
    //<editor-fold desc="Constructors">
    Series::Series():Series(nullptr, false, "") {}

    Series::Series(std::shared_ptr<arrow::Array> const &arr, bool isIndex, std::string name)
            : NDFrame<arrow::Array>(arr, nullptr, true), m_name(std::move(name)) {
        if (isIndex) {
            setIndexer();
        } else if (!m_array) {
            m_index = arrow::MakeArrayOfNull(arrow::uint64(), 0).MoveValueUnsafe();
            m_array = arrow::MakeArrayOfNull(arrow::float64(), 0).MoveValueUnsafe();
        } else {
            m_index = uint_range(arr->length());
        }
    }

    Series::Series(
            std::shared_ptr<arrow::Array> const &arr,
            std::shared_ptr<arrow::Array> const &index,
            std::string name,
            bool skipIndex)
            : NDFrame<arrow::Array>(arr, index, skipIndex), m_name(std::move(name)) {
        if (!m_array) {
            m_index = arrow::MakeArrayOfNull(arrow::uint64(), 0).MoveValueUnsafe();
            m_array = arrow::MakeArrayOfNull(arrow::float64(), 0).MoveValueUnsafe();
        }
    }
    //</editor-fold>

    //<editor-fold desc="Indexing Functions">
    Scalar Series::operator[](int64_t index) const {
        auto &&result = m_array->GetScalar(index >= 0 ? index : m_array->length() + index);

        if (result.ok()) {
            return pd::Scalar{std::move(result.ValueUnsafe())};
        } else {
            throw std::runtime_error(result.status().ToString());
        }
    }

    Series Series::operator[](Slice slice) const {
        if (isIndex) {
            throw std::runtime_error(std::string(__FUNCTION__).append(" is not allowed for Index"));
        }
        slice.normalize(size());
        return {slice.end == 0 ? m_array->Slice(slice.start) : m_array->Slice(slice.start, slice.end - slice.start),
                slice.end == 0 ? m_index->Slice(slice.start) : m_index->Slice(slice.start, slice.end - slice.start),
                m_name};
    }

    Series Series::where(const Series &x) const {
        if (isIndex) {
            throw std::runtime_error("where is not allowed on index Series");
        }
        if (x.dtype()->id() != arrow::Type::BOOL) {
            throw std::runtime_error("non bool type is  valid for where() use take()");
        }

        arrow::compute::FilterOptions opt{arrow::compute::FilterOptions::NullSelectionBehavior::EMIT_NULL};
        auto arr = ReturnOrThrowOnFailure(
                arrow::compute::CallFunction("array_filter", {m_array, x.m_array}, &opt)).make_array();
        auto idx = ReturnOrThrowOnFailure(
                arrow::compute::CallFunction("array_filter", {m_index, x.m_array})).make_array();
        return {arr, idx};
    }

    Series Series::take(const Series &x) const {
        if (isIndex) {
            throw std::runtime_error("take is not allowed on index Series");
        }
        if (x.dtype()->id() == arrow::Type::BOOL) {
            throw std::runtime_error("bool type is not valid for take() use where()");
        }

        auto arr = ReturnOrThrowOnFailure(
                arrow::compute::CallFunction("array_take", {m_array, x.m_array})).make_array();
        auto idx = ReturnOrThrowOnFailure(
                arrow::compute::CallFunction("array_take", {m_index, x.m_array})).make_array();
        return {arr, idx};
    }

    //</editor-fold>

    std::shared_ptr<arrow::DataType> Series::dtype() const {
        return m_array->type();
    }

    uint64_t Series::nbytes() const {
        return m_array->data()->buffers[1]->size();
    }

    int64_t Series::size() const {
        return m_array->length();
    }

    bool Series::empty() const {
        return (m_array == nullptr) or (m_array->length() == 0);
    }

    void Series::add_prefix(std::string prefix) {
        m_name = prefix.append(m_name);
    }

    void Series::add_suffix(std::string const &suffix) {
        m_name.append(suffix);
    }

    void Series::rename(std::string const &newName) {
        m_name = newName;
    }

    std::string Series::name() const {
        return m_name;
    }

    DataFrame Series::toFrame(std::optional<std::string> const &name) const {
        auto field = arrow::field(name.value_or(m_name), dtype());
        return {arrow::schema({field}), size(), {m_array}, m_index};
    }

    BINARY_OPERATOR(/, divide)

    BINARY_OPERATOR(+, add)

    BINARY_OPERATOR(-, subtract)

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

    Series Series::operator-() const {
        return ReturnSeriesOrThrowOnError(arrow::compute::Negate(m_array, arrow::compute::ArithmeticOptions{false}));
    }

    Series Series::pow(double x) const {
        auto result = arrow::compute::Power({m_array}, arrow::MakeScalar(x));
        if (result.ok()) {
            return {result->make_array(), false, m_name};
        } else {
            throw std::runtime_error(result.status().ToString());
        }
    }

    Series Series::exp() const {
        return ReturnSeriesOrThrowOnError( arrow::compute::Exp({m_array}));
    }

    GenericFunctionSeriesReturn(sqrt)

    GenericFunctionSeriesReturn(sign)

    GenericFunctionSeriesReturn(ln)

    GenericFunctionSeriesReturn(log10)

    GenericFunctionSeriesReturn(log1p)

    GenericFunctionSeriesReturn(log2)

    GenericFunctionSeriesReturn(cos)

    GenericFunctionSeriesReturn(sin)

    GenericFunctionSeriesReturn(tan)

    GenericFunctionSeriesReturn(ceil)

    GenericFunctionSeriesReturn(floor)

    GenericFunctionSeriesReturn(round)

    GenericFunctionSeriesReturn(trunc)

    Series Series::round_to_multiple(double multiple, arrow::compute::RoundMode roundMode) const {
        arrow::compute::RoundToMultipleOptions opt{multiple, roundMode};
        return ReturnSeriesOrThrowOnError(arrow::compute::RoundToMultiple({m_array}, opt));
    }

    Series Series::logb(int base) const {
        return ReturnSeriesOrThrowOnError(arrow::compute::Logb({m_array}, arrow::MakeScalar(base)));
    }

    GenericFunctionSeriesReturnRename(operator~, bit_wise_not, Series)

    GenericFunctionSeriesReturnRename(operator!, invert, Series)

    Series Series::cumsum(double start, bool skip_nulls) const {
        return ReturnSeriesOrThrowOnError(
                arrow::compute::CumulativeSum(m_array, arrow::compute::CumulativeOptions{start, skip_nulls}));
    }

    Series Series::cumprod(double start, bool skip_nulls) const {
        return ReturnSeriesOrThrowOnError(
                arrow::compute::CumulativeProd(m_array, arrow::compute::CumulativeOptions{start, skip_nulls}));
    }

    Series Series::cummax(double start, bool skip_nulls) const {
        return ReturnSeriesOrThrowOnError(
                arrow::compute::CumulativeMax(m_array, arrow::compute::CumulativeOptions{start, skip_nulls}));
    }

    Series Series::cummin(double start, bool skip_nulls) const {
        return ReturnSeriesOrThrowOnError(
                arrow::compute::CumulativeMin(m_array, arrow::compute::CumulativeOptions{start, skip_nulls}));
    }

    std::shared_ptr<arrow::DictionaryArray> Series::dictionary_encode() const {

        auto result = arrow::compute::DictionaryEncode(m_array);
        if (result.ok()) {
            return result.MoveValueUnsafe().array_as<arrow::DictionaryArray>();
        } else {
            throw std::runtime_error(result.status().ToString());
        }
    }

    Resampler Series::resample(
            std::string const &rule,
            bool closed_right,
            bool label_right,
            TimeGrouperOrigin const &origin,
            time_duration const &offset,
            std::string const &tz) const {
        return pd::resample(*this, rule, closed_right, label_right, origin, offset, tz);
    }

    GenericFunctionSeriesReturn(unique)

    GenericFunctionSeriesReturnRename(drop_na, drop_null, Series)

    GenericFunctionSeriesReturn(indices_nonzero)

    GenericFunctionSeriesReturnString(ascii_capitalize)

    GenericFunctionSeriesReturnString(
            ascii_lower)

    GenericFunctionSeriesReturnString(ascii_reverse)

    GenericFunctionSeriesReturnString(ascii_swapcase)

    GenericFunctionSeriesReturnString(ascii_title)

    GenericFunctionSeriesReturnString(ascii_upper)

    GenericFunctionScalarReturnString(binary_length)

    GenericFunctionSeriesReturnString(
            binary_repeat)

    GenericFunctionSeriesReturnString(binary_reverse)

    GenericFunctionSeriesReturnString(utf8_capitalize)

    GenericFunctionSeriesReturnString(utf8_length)

    GenericFunctionSeriesReturnString(utf8_lower)

    GenericFunctionSeriesReturnString(utf8_reverse)

    GenericFunctionSeriesReturnString(utf8_swapcase)

    GenericFunctionSeriesReturnString(
            utf8_title)

    GenericFunctionSeriesReturnString(utf8_upper)

    GenericFunctionSeriesReturnString(ascii_is_alnum)

    GenericFunctionSeriesReturnString(
            ascii_is_alpha)

    GenericFunctionSeriesReturnString(ascii_is_decimal)

    GenericFunctionSeriesReturnString(ascii_is_lower)

    GenericFunctionSeriesReturnString(
            utf8_is_alnum)

    GenericFunctionSeriesReturnString(utf8_is_alpha)

    GenericFunctionSeriesReturnString(utf8_is_decimal)

    GenericFunctionSeriesReturnString(
            utf8_is_digit)

    GenericFunctionSeriesReturnString(utf8_is_lower)

    GenericFunctionSeriesReturnString(
            utf8_is_numeric)

    GenericFunctionSeriesReturnString(utf8_is_printable)

    GenericFunctionSeriesReturnString(
            utf8_is_space)

    GenericFunctionSeriesReturnString(utf8_is_upper)

    GenericFunctionSeriesReturnString(
            ascii_is_title)

    GenericFunctionSeriesReturnString(utf8_is_title)

    GenericFunctionSeriesReturnString(string_is_ascii)

    GenericFunctionSeriesReturnString(ascii_ltrim_whitespace)

    GenericFunctionSeriesReturnString(ascii_rtrim_whitespace)

    GenericFunctionSeriesReturnString(ascii_trim_whitespace)

    GenericFunctionSeriesReturnString(utf8_ltrim_whitespace)

    GenericFunctionSeriesReturnString(
            utf8_rtrim_whitespace)

    GenericFunctionSeriesReturnString(
            utf8_trim_whitespace)

    StringLike Series::str() const {
        if (arrow::is_binary_like(m_array->type_id())) {
            return {m_array};
        }
        return {this->cast(std::make_shared<arrow::StringType>()).m_array};
    }

    DateTimeLike Series::dt() const {
        if (m_array->type_id() == arrow::Type::TIMESTAMP) {
            return {m_array};
        }
        return {this->cast(std::make_shared<arrow::TimestampType>(arrow::TimeUnit::NANO)).m_array};
    }

    Series StringLike::binary_replace_slice(int64_t start, int64_t stop, std::string const &replacement) const {
        arrow::compute::ReplaceSliceOptions opt(start, stop, replacement);
        return ReturnSeriesOrThrowOnError(arrow::compute::CallFunction("binary_replace_slice", {m_array}, &opt));
    }

    Series StringLike::replace_substring(
            std::string const &pattern,
            std::string const &replacement,
            int64_t const &max_replacements) const {
        arrow::compute::ReplaceSubstringOptions opt(pattern, replacement, max_replacements);
        return ReturnSeriesOrThrowOnError(arrow::compute::CallFunction("replace_substring", {m_array}, &opt));
    }

    Series StringLike::replace_substring_regex(
            std::string const &pattern,
            std::string const &replacement,
            int64_t const &max_replacements) const {
        arrow::compute::ReplaceSubstringOptions opt(pattern, replacement, max_replacements);
        return ReturnSeriesOrThrowOnError(arrow::compute::CallFunction("replace_substring_regex", {m_array}, &opt));
    }

    Series StringLike::utf8_replace_slice(int64_t start, int64_t stop, std::string const &replacement) const {
        arrow::compute::ReplaceSliceOptions opt(start, stop, replacement);
        return ReturnSeriesOrThrowOnError(arrow::compute::CallFunction("utf8_replace_slice", {m_array}, &opt));
    }

    Series StringLike::ascii_center(int64_t width, std::string const &padding) const {
        arrow::compute::PadOptions opt(width, padding);
        return ReturnSeriesOrThrowOnError(arrow::compute::CallFunction("ascii_center", {m_array}, &opt));
    }

    Series StringLike::ascii_lpad(int64_t width, std::string const &padding) const {
        arrow::compute::PadOptions opt(width, padding);
        return ReturnSeriesOrThrowOnError(arrow::compute::CallFunction("ascii_lpad", {m_array}, &opt));
    }

    Series StringLike::ascii_rpad(int64_t width, std::string const &padding) const {
        arrow::compute::PadOptions opt(width, padding);
        return ReturnSeriesOrThrowOnError(arrow::compute::CallFunction("ascii_rpad", {m_array}, &opt));
    }

    Series StringLike::utf8_center(int64_t width, std::string const &padding) const {
        arrow::compute::PadOptions opt(width, padding);
        return ReturnSeriesOrThrowOnError(arrow::compute::CallFunction("utf8_center", {m_array}, &opt));
    }

    Series StringLike::utf8_lpad(int64_t width, std::string const &padding) const {
        arrow::compute::PadOptions opt(width, padding);
        return ReturnSeriesOrThrowOnError(arrow::compute::CallFunction("utf8_lpad", {m_array}, &opt));
    }

    Series StringLike::utf8_rpad(int64_t width, std::string const &padding) const {
        arrow::compute::PadOptions opt(width, padding);
        return ReturnSeriesOrThrowOnError(arrow::compute::CallFunction("utf8_rpad", {m_array}, &opt));
    }

    Series StringLike::ascii_split_whitespace(int64_t max_splits, bool reverse) const {
        arrow::compute::SplitOptions opt(max_splits, reverse);
        return ReturnSeriesOrThrowOnError(arrow::compute::CallFunction("ascii_split_whitespace", {m_array}, &opt));
    }

    Series StringLike::utf8_split_whitespace(int64_t max_splits, bool reverse) const {
        arrow::compute::SplitOptions opt(max_splits, reverse);
        return ReturnSeriesOrThrowOnError(arrow::compute::CallFunction("utf8_split_whitespace", {m_array}, &opt));
    }

    Series StringLike::split_pattern(std::string const &pattern, int64_t max_splits, bool reverse) const {
        arrow::compute::SplitPatternOptions opt(pattern, max_splits, reverse);
        return ReturnSeriesOrThrowOnError(arrow::compute::CallFunction("split_pattern", {m_array}, &opt));
    }

    Series StringLike::split_pattern_regex(std::string const &pattern, int64_t max_splits, bool reverse) const {
        arrow::compute::SplitPatternOptions opt(pattern, max_splits, reverse);
        return ReturnSeriesOrThrowOnError(arrow::compute::CallFunction("split_pattern_regex", {m_array}, &opt));
    }

    Series StringLike::ascii_ltrim(const std::string &characters) const {
        arrow::compute::TrimOptions opt(characters);
        return ReturnSeriesOrThrowOnError(arrow::compute::CallFunction("ascii_ltrim", {m_array}, &opt));
    }

    Series StringLike::ascii_rtrim(const std::string &characters) const {
        arrow::compute::TrimOptions opt(characters);
        return ReturnSeriesOrThrowOnError(arrow::compute::CallFunction("ascii_rtrim", {m_array}, &opt));
    }

    Series StringLike::ascii_trim(const std::string &characters) const {
        arrow::compute::TrimOptions opt(characters);
        return ReturnSeriesOrThrowOnError(arrow::compute::CallFunction("ascii_trim", {m_array}, &opt));
    }

    Series StringLike::utf8_ltrim(const std::string &characters) const {
        arrow::compute::TrimOptions opt(characters);
        return ReturnSeriesOrThrowOnError(arrow::compute::CallFunction("utf8_ltrim", {m_array}, &opt));
    }

    Series StringLike::utf8_rtrim(const std::string &characters) const {
        arrow::compute::TrimOptions opt(characters);
        return ReturnSeriesOrThrowOnError(arrow::compute::CallFunction("utf8_rtrim", {m_array}, &opt));
    }

    Series StringLike::utf8_trim(const std::string &characters) const {
        arrow::compute::TrimOptions opt(characters);
        return ReturnSeriesOrThrowOnError(arrow::compute::CallFunction("utf8_trim", {m_array}, &opt));
    }

    Series StringLike::extract_regex(const std::string &pattern) const {
        arrow::compute::ExtractRegexOptions opt(pattern);
        return ReturnSeriesOrThrowOnError(arrow::compute::CallFunction("extract_regex", {m_array}, &opt));
    }

    Series StringLike::binary_join(const Series &joiner) const {
        return ReturnSeriesOrThrowOnError(arrow::compute::CallFunction("binary_join", {m_array, joiner.m_array}));
    }

    Series StringLike::binary_join(const Scalar &joiner) const {
        return ReturnSeriesOrThrowOnError(arrow::compute::CallFunction("binary_join", {m_array, joiner.scalar}));
    }

    Series StringLike::utf8_slice_codeunits(int64_t start, int64_t stop, int64_t step) const {
        arrow::compute::SliceOptions opt(start, stop, step);
        return ReturnSeriesOrThrowOnError(arrow::compute::CallFunction("utf8_slice_codeunits", {m_array}, &opt));
    }

    Series StringLike::count_substring(const std::string &pattern, bool ignore_case) {
        arrow::compute::MatchSubstringOptions opt(pattern, ignore_case);
        return ReturnSeriesOrThrowOnError(arrow::compute::CallFunction("count_substring", {m_array}, &opt));
    }

    Series StringLike::count_substring_regex(const std::string &pattern, bool ignore_case) {
        arrow::compute::MatchSubstringOptions opt(pattern, ignore_case);
        return ReturnSeriesOrThrowOnError(arrow::compute::CallFunction("count_substring_regex", {m_array}, &opt));
    }

    Series StringLike::ends_with(const std::string &pattern, bool ignore_case) {
        arrow::compute::MatchSubstringOptions opt(pattern, ignore_case);
        return ReturnSeriesOrThrowOnError(arrow::compute::CallFunction("ends_with", {m_array}, &opt));
    }

    Series StringLike::find_substring(const std::string &pattern, bool ignore_case) {
        arrow::compute::MatchSubstringOptions opt(pattern, ignore_case);
        return ReturnSeriesOrThrowOnError(arrow::compute::CallFunction("find_substring", {m_array}, &opt));
    }

    Series StringLike::match_like(const std::string &pattern, bool ignore_case) {
        arrow::compute::MatchSubstringOptions opt(pattern, ignore_case);
        return ReturnSeriesOrThrowOnError(arrow::compute::CallFunction("match_like", {m_array}, &opt));
    }

    Series StringLike::match_substring_regex(const std::string &pattern, bool ignore_case) {
        arrow::compute::MatchSubstringOptions opt(pattern, ignore_case);
        return ReturnSeriesOrThrowOnError(arrow::compute::CallFunction("match_substring_regex", {m_array}, &opt));
    }

    Series StringLike::match_substring(const std::string &pattern, bool ignore_case) {
        arrow::compute::MatchSubstringOptions opt(pattern, ignore_case);
        return ReturnSeriesOrThrowOnError(arrow::compute::CallFunction("match_substring", {m_array}, &opt));
    }

    Series StringLike::starts_with(const std::string &pattern, bool ignore_case) {
        arrow::compute::MatchSubstringOptions opt(pattern, ignore_case);
        return ReturnSeriesOrThrowOnError(arrow::compute::CallFunction("starts_with", {m_array}, &opt));
    }

    Series StringLike::index_in(const Series &value_set, bool skip_nulls) {
        arrow::compute::SetLookupOptions opt(value_set.array(), skip_nulls);
        return ReturnSeriesOrThrowOnError(arrow::compute::CallFunction("index_in", {m_array}, &opt));
    }

    Series StringLike::is_in(const Series &value_set, bool skip_nulls) {
        arrow::compute::SetLookupOptions opt(value_set.array(), skip_nulls);
        return ReturnSeriesOrThrowOnError(arrow::compute::CallFunction("is_in", {m_array}, &opt));
    }

    Series DateTimeLike::ceil(
            int multiple,
            arrow::compute::CalendarUnit unit,
            bool week_starts_monday,
            bool ceil_is_strictly_greater,
            bool calendar_based_origin) const {
        arrow::compute::RoundTemporalOptions opt(
                multiple,
                unit,
                week_starts_monday,
                ceil_is_strictly_greater,
                calendar_based_origin);
        return ReturnSeriesOrThrowOnError(arrow::compute::CallFunction("ceil_temporal", {m_array}, &opt));
    }

    Series DateTimeLike::floor(
            int multiple,
            arrow::compute::CalendarUnit unit,
            bool week_starts_monday,
            bool ceil_is_strictly_greater,
            bool calendar_based_origin) const {
        arrow::compute::RoundTemporalOptions opt(
                multiple,
                unit,
                week_starts_monday,
                ceil_is_strictly_greater,
                calendar_based_origin);
        return ReturnSeriesOrThrowOnError(arrow::compute::CallFunction("floor_temporal", {m_array}, &opt));
    }

    Series DateTimeLike::round(
            int multiple,
            arrow::compute::CalendarUnit unit,
            bool week_starts_monday,
            bool ceil_is_strictly_greater,
            bool calendar_based_origin) const {
        arrow::compute::RoundTemporalOptions opt(
                multiple,
                unit,
                week_starts_monday,
                ceil_is_strictly_greater,
                calendar_based_origin);
        return ReturnSeriesOrThrowOnError(arrow::compute::CallFunction("round_temporal", {m_array}, &opt));
    }

    Series Series::cast(const std::shared_ptr<arrow::DataType> &dt, bool safe) const {
        return ReturnSeriesOrThrowOnError(arrow::compute::Cast(m_array, dt, arrow::compute::CastOptions{safe}));
    }

    Series Series::strftime(const std::string &format, std::string const &locale) const {
        return ReturnSeriesOrThrowOnError(
                arrow::compute::Strftime(m_array, arrow::compute::StrftimeOptions{format, locale}));
    }

    Series Series::strptime(const std::string &format, arrow::TimeUnit::type unit, bool error_is_null) const {
        return ReturnSeriesOrThrowOnError(
                arrow::compute::Strptime(m_array, arrow::compute::StrptimeOptions{format, unit, error_is_null}));
    }


    Series Series::shift(int32_t periods, std::optional<pd::Scalar> const &fillValue) const {
        if (periods == 0) {
            return *this;
        }

        auto scalarArrayBuilder = pd::ReturnOrThrowOnFailure(arrow::MakeBuilder(m_array->type()));
        pd::ThrowOnFailure(scalarArrayBuilder->Reserve(m_array->length()));

        bool shiftRight = periods > 0;
        periods = std::abs(periods);
        auto newLength = m_array->length() - periods;
        auto shift = [&]() {
            if (fillValue) {
                ThrowOnFailure(
                        scalarArrayBuilder->AppendScalars(std::vector<ScalarPtr>(periods, fillValue->scalar)));
            } else {
                ThrowOnFailure(scalarArrayBuilder->AppendNulls(periods));
            }
        };

        if (shiftRight) {
            shift();
            ThrowOnFailure(scalarArrayBuilder->AppendArraySlice(arrow::ArraySpan(*m_array->data()), 0, newLength));
        } else {
            ThrowOnFailure(
                    scalarArrayBuilder->AppendArraySlice(arrow::ArraySpan(*m_array->data()), periods, newLength));
            shift();
        }

        return {
                pd::ReturnOrThrowOnFailure(scalarArrayBuilder->Finish()),
                m_index,
                m_name
        };
    }

    bool Series::is_valid(int row) const {
        return pd::ReturnOrThrowOnFailure(m_array->GetScalar(row))->is_valid;
    }

    Series Series::pct_change(int64_t period) const {
        hmdf::roc_v<double> visitor{static_cast<size_t>(period)};
        auto result = hmVisit(visitor).get_result();
        return Series{result, m_name, m_index};
    }

    GenericFunctionSeriesReturnRename(ffill, fill_null_forward, Series)

    GenericFunctionSeriesReturnRename(bfill, fill_null_backward, Series)

    Series Series::replace_with_mask(Series const &cond, Series const &other) const {
        if ((cond.size() == other.size()) and (other.size() <= this->size())) {
            return ReturnSeriesOrThrowOnError(arrow::compute::ReplaceWithMask(m_array, cond.m_array, other.m_array));
        } else {
            throw std::runtime_error(
                    ""
                    "replace_with_mask error: valid precondition "
                    "(cond.size() == other.size()) and (other.size() <= this->size())");
        }
    }

    Series Series::intersection(Series const &other) const {
        if (!isIndex || !other.isIndex) {
            throw std::runtime_error("Both Series must be indexes for intersection to be valid.");
        }

        arrow::Int64Builder intersection_indices;
        std::vector<int64_t> result;
        result.reserve(indexer.size());
        for (auto const &[index, value]: indexer) {
            if (other.indexer.count(index) == 1) {
                result.push_back(value);
            }
        }
        std::sort(result.begin(), result.end());
        ThrowOnFailure(intersection_indices.AppendValues(result));

        auto intersection_array =
                ReturnOrThrowOnFailure(arrow::compute::Take(m_array, intersection_indices.Finish().MoveValueUnsafe()));
        return {intersection_array.make_array(), true};
    }

    Series Series::union_(const Series &other) const {
        if (!isIndex || !other.isIndex) {
            throw std::runtime_error("Both Series objects must be indexes to perform union.");
        }

        if (!isIndex || !other.isIndex) {
            throw std::runtime_error("Both Series must be indexes to perform union operation.");
        }
        auto concatenated_arrays = pd::ReturnOrThrowOnFailure(arrow::Concatenate({m_array, other.m_array}));

        auto new_series =
                Series(pd::ReturnOrThrowOnFailure(arrow::compute::Unique(concatenated_arrays)), nullptr, m_name, true);
        new_series.setIndexer();

        return new_series;
    }

    std::vector<std::shared_ptr<arrow::Scalar>> Series::to_vector() const {
        std::vector<std::shared_ptr<arrow::Scalar>> vec;
        vec.reserve(size());
        for (int i = 0; i < size(); i++) {
            vec.push_back(m_array->GetScalar(i).ValueUnsafe());
        }
        return vec;
    }

    std::vector<std::shared_ptr<arrow::Scalar>> Series::get_indexed_values() const {
        if (!isIndex) {
            throw std::runtime_error("Cannot get indexed values from non-index series");
        }
        auto key_view = indexer | std::views::keys;
        return {key_view.begin(), key_view.end()};
    }

    Series Series::append(const Series &to_append, bool ignore_index) const {
        if (!ignore_index) {
            if (isIndex != to_append.isIndex) {
                throw std::runtime_error("Index type must be the same for both series.");
            }
            if (isIndex && to_append.isIndex) {
                if (intersection(to_append).size() > 0) {
                    throw std::runtime_error("Index must be the unique for both series if series is an Index.");
                }
            }
            if (not isIndex) {
                if ((m_index == nullptr && to_append.m_index != nullptr) ||
                    (m_index != nullptr && to_append.m_index == nullptr)) {
                    throw std::runtime_error(
                            "If ignore_index is not set both indexes must either have values or both be null");
                } else if (m_index == nullptr && to_append.m_index == nullptr) {
                    ignore_index = true;
                }
            }

            auto concatenated_arrays = arrow::Concatenate({m_array, to_append.m_array}).MoveValueUnsafe();

            if (isIndex) {
                Indexer new_indexer;
                int i = 0;
                for (i = 0; i < concatenated_arrays->length(); i++) {
                    new_indexer[concatenated_arrays->GetScalar(i).MoveValueUnsafe()] = i;
                }

                auto new_series = Series(concatenated_arrays, nullptr, m_name, true);
                new_series.indexer = new_indexer;
                new_series.isIndex = true;

                return new_series;
            } else {
                return Series(
                        concatenated_arrays,
                        arrow::Concatenate({m_index, to_append.m_index}).MoveValueUnsafe(),
                        m_name);
            }
        } else {
            auto concatenated_arrays = arrow::Concatenate({m_array, to_append.m_array}).MoveValueUnsafe();
            return Series(concatenated_arrays, nullptr, m_name);
        }
    }

    Series Series::argsort(bool ascending) const {
        auto opt = arrow::compute::ArraySortOptions{ascending ? arrow::compute::SortOrder::Ascending :
                                                    arrow::compute::SortOrder::Descending};
        return ReturnSeriesOrThrowOnError(arrow::compute::CallFunction("array_sort_indices", {m_array}, &opt));
    }

    double Series::cov(const Series & /*s2*/) const {
        throw std::runtime_error("currently lacking implementation");
    }

    Series Series::clip(Series const &x, pd::Scalar const &min, pd::Scalar const &max, bool skipNull) const {
        arrow::compute::ElementWiseAggregateOptions option{skipNull};
        return {pd::ReturnOrThrowOnFailure(arrow::compute::MaxElementWise(
                {pd::ReturnOrThrowOnFailure(
                        arrow::compute::MinElementWise({x.m_array, arrow::Datum(max.scalar)}, option)),
                 arrow::Datum(min.scalar)}, option)).make_array(), x.indexArray()};
    }

    double Series::corr(const Series & /*ununsed*/, CorrelationType /*ununsed*/) const {
        throw std::runtime_error("currently lacking implementation");
    }

    double Series::corr(const Series & /*unused*/, double (*/*unused*/)(double)) const {
        throw std::runtime_error("Series currently only supports Pearson CorrelationType");
    }

    std::ostream &operator<<(std::ostream &os, Series const &series) {
        auto N = int(series.size());
        int maxDigit = 0;

        while (N > 0) {
            N = N / 10;
            maxDigit++;
        }
        for (int i = 0; i < series.size(); i++) {
            auto index_status = (series.m_index == nullptr) ? arrow::MakeScalar(i) : series.m_index->GetScalar(i);
            if (index_status.ok())
                os << std::setw(maxDigit) << index_status.MoveValueUnsafe()->ToString() << "\t"
                   << series[i].scalar->ToString() << "\n";
            else
                throw std::runtime_error(index_status.status().ToString());
        }

        if (not series.m_name.empty())
            os << "\nname: " << series.name();

        os << "\ttype: " << series.m_array->type()->ToString() << "\t";
        return os;
    }

    Series
    Series::ewm(EWMAgg agg, double value, EWMAlphaType type, bool adjust, bool ignore_na, int min_periods, bool bias)
    const {
        min_periods = std::max(min_periods, 1);
        double comass{};
        switch (type) {
            case EWMAlphaType::CenterOfMass:
                if (value >= 0) {
                    comass = value;
                } else {
                    throw std::runtime_error("EWM with COM require Value >= 0");
                }
                break;
            case EWMAlphaType::Span:
                if (value >= 1) {
                    comass = (value - 1) / 2;
                } else {
                    throw std::runtime_error("EWM with Span require Value >= 1");
                }
                break;
            case EWMAlphaType::HalfLife:
                if (value > 0) {
                    double decay = 1 - std::exp(std::log(0.5) / value);
                    comass = 1 / decay - 1;
                }
                throw std::runtime_error("EWM is not supported");
                break;
            case EWMAlphaType::Alpha:
                if (value > 0 and value <= 1) {
                    comass = (1 - value) / value;
                } else {
                    throw std::runtime_error("EWM with Alpha require 0 < value <= 1");
                }
                break;
        };

        auto casted = ReturnOrThrowOnFailure(arrow::compute::Cast(m_array, DoubleTypePtr)).make_array();
        auto doubleArray = arrow::internal::checked_pointer_cast<arrow::DoubleArray>(casted);

        std::vector<double> output;

        const std::vector<int64_t> begin{0};
        const std::vector<int64_t> end{doubleArray->length()};
        switch (agg) {
            case EWMAgg::Mean:
                output = ewm(doubleArray, begin, end, min_periods, comass, adjust, ignore_na, {});
                break;
            case EWMAgg::Var:
            case EWMAgg::StdDev:
                output = ewmcov(doubleArray, begin, end, doubleArray, min_periods, comass, adjust, ignore_na, bias);
                if (agg == EWMAgg::StdDev) {
                    std::ranges::transform(output, output.begin(),
                                           [](auto const &x) { return x < 0 ? 0 : std::sqrt(x); });
                }
                break;
        }
        return Series{output, m_name, m_index};
    }

    Series Series::nth_element(int n) const {
        auto opt = arrow::compute::PartitionNthOptions{n};
        return ReturnSeriesOrThrowOnError(arrow::compute::CallFunction("partition_nth_indices", {m_array}, &opt));
    }

    std::array<std::shared_ptr<arrow::Array>, 2> Series::sort(bool ascending) const {
        auto opt = arrow::compute::ArraySortOptions{ascending ? arrow::compute::SortOrder::Ascending :
                                                    arrow::compute::SortOrder::Descending};
        auto result = arrow::compute::CallFunction("array_sort_indices", {m_array}, &opt);
        if (result.ok()) {
            auto indices = result.MoveValueUnsafe();
            return {arrow::compute::Take(m_array, indices)->make_array(), indices.make_array()};
        }
        throw std::runtime_error(result.status().ToString());
    }

    std::vector<double> Series::ewm(
            const std::shared_ptr<arrow::DoubleArray> &vals,
            const std::vector<int64_t> &start,
            const std::vector<int64_t> &end,
            int minp,
            double com,
            bool adjust,
            bool ignore_na,
            const std::vector<double> &deltas,
            bool normalize) {
        const size_t N = vals->length();
        const int64_t M = start.size();

        std::vector<double> output(N);
        if (N == 0) {
            return output;
        }

        bool use_deltas = !deltas.empty();
        double alpha = 1. / (1. + com);
        double old_wt_factor = 1. - alpha;
        double new_wt = adjust ? 1. : alpha;

        for (int64_t j: std::views::iota(0L, M)) {
            const int64_t s = start[j];
            const int64_t e = end[j];

            const int64_t win_size = e - s;
            const auto sub_vals = arrow::checked_pointer_cast<arrow::DoubleArray>(vals->Slice(s, win_size));

            std::span<double> sub_output(output.begin() + s, output.begin() + e);

            auto weighted = (*vals)[0];
            bool is_observation = weighted.has_value();
            int nobs = int(is_observation);
            sub_output[0] = (nobs >= minp) ? *weighted : NAN;
            double old_wt = 1.;

            for (size_t i = 1; i < N; i++) {
                auto cur = (*vals)[i];
                is_observation = cur.has_value();
                nobs += is_observation;
                if (weighted.has_value()) {
                    if (is_observation || not ignore_na) {
                        if (normalize) {
                            if (use_deltas) {
                                old_wt *= std::pow(old_wt_factor, deltas[i - 1]);
                            } else {
                                old_wt *= old_wt_factor;
                            }
                        } else {
                            weighted = old_wt_factor * weighted.value();
                        }
                        if (is_observation) {
                            if (normalize) {
                                // avoid numerical errors on constant series
                                if (weighted != cur) {
                                    weighted = old_wt * weighted.value() + new_wt * cur.value();
                                    weighted.value() /= (old_wt + new_wt);
                                }
                                if (adjust) {
                                    old_wt += new_wt;
                                } else {
                                    old_wt = 1.0;
                                }
                            } else {
                                weighted.value() += cur.value();
                            }
                        }
                    }
                } else if (is_observation) {
                    weighted = cur;
                }
                sub_output[i] = (nobs >= minp) ? *weighted : std::numeric_limits<double>::quiet_NaN();
            }
        }
        return output;
    }

    std::vector<double> Series::ewmcov(
            const std::shared_ptr<arrow::DoubleArray> &input_x,
            const std::vector<int64_t> &start,
            const std::vector<int64_t> &end,
            const std::shared_ptr<arrow::DoubleArray> &input_y,
            int minp,
            double com,
            bool adjust,
            bool ignore_na,
            bool bias) {
        const size_t N = input_x->length();
        const size_t M = input_y->length();

        if (M != N) {
            std::stringstream ss;
            ss << "arrays are of different lengths (" << N << " and " << M << ")";
            throw std::runtime_error(ss.str());
        }

        std::vector<double> output(N);
        if (N == 0) {
            return output;
        }

        const double alpha = 1.0 / (1.0 + com);
        const double old_wt_factor = 1.0 - alpha;
        const double new_wt = adjust ? 1.0 : alpha;

        const size_t L = start.size();
        for (size_t j: std::views::iota(0UL, L)) {
            const int64_t s = start[j];
            const int64_t e = end[j];

            const int64_t win_size = e - s;
            const auto sub_x_vals = arrow::checked_pointer_cast<arrow::DoubleArray>(input_x->Slice(s, win_size));
            const auto sub_y_vals = arrow::checked_pointer_cast<arrow::DoubleArray>(input_y->Slice(s, win_size));

            std::span<double> sub_out(output.begin() + s, output.begin() + e);
            double mean_x{std::nan("")}, mean_y{std::nan("")};

            std::optional<double> mean_x_check = (*sub_x_vals)[0];
            std::optional<double> mean_y_check = (*sub_y_vals)[0];

            bool is_observation = mean_x_check and mean_y_check;
            int nobs = int(is_observation);
            if (is_observation) {
                mean_x = *mean_x_check;
                mean_y = *mean_y_check;
            }

            sub_out[0] = (nobs >= minp) ? (bias ? 0.0 : std::nan("")) : std::nan("");

            double cov = 0., sum_wt = 1., sum_wt2 = 1., old_wt = 1.;
            double old_mean_x{}, old_mean_y{};
            for (int64_t i: std::views::iota(1L, win_size)) {
                std::optional<double> cur_x_check = (*sub_x_vals)[i], cur_y_check = (*sub_y_vals)[i];
                is_observation = (cur_x_check and cur_y_check);
                nobs += is_observation;

                if (mean_x_check) {
                    mean_x = *mean_x_check;
                    mean_y = *mean_y_check;

                    if (is_observation or not ignore_na) {
                        double cur_x = cur_x_check.value(), cur_y = cur_y_check.value();

                        sum_wt *= old_wt_factor;
                        sum_wt2 *= (old_wt_factor * old_wt_factor);
                        old_wt *= old_wt_factor;

                        if (is_observation) {
                            old_mean_x = mean_x;
                            old_mean_y = mean_y;

                            if (mean_x != cur_x) {
                                mean_x_check = ((old_wt * old_mean_x) + (new_wt * cur_x)) / (old_wt + new_wt);
                            }

                            if (mean_y != cur_y) {
                                mean_y_check = ((old_wt * old_mean_y) + (new_wt * cur_y)) / (old_wt + new_wt);
                            }

                            cov = ((old_wt *
                                    (cov +
                                     ((old_mean_x - mean_x_check.value()) * (old_mean_y - mean_y_check.value())))) +
                                   (new_wt * ((cur_x - mean_x_check.value()) * (cur_y - mean_y_check.value())))) /
                                  (old_wt + new_wt);
                            sum_wt += new_wt;
                            sum_wt2 += (new_wt * new_wt);
                            old_wt += new_wt;

                            if (not adjust) {
                                sum_wt /= old_wt;
                                sum_wt2 /= (old_wt * old_wt);
                                old_wt = 1.;
                            }
                        }
                    }
                } else if (is_observation) {
                    mean_x_check = cur_x_check;
                    mean_y_check = cur_y_check;
                }

                if (nobs >= minp) {
                    if (not bias) {
                        const double numerator = sum_wt * sum_wt;
                        const double denominator = numerator - sum_wt2;

                        if (denominator > 0) {
                            sub_out[i] = (numerator / denominator) * cov;
                        } else {
                            sub_out[i] = std::nan("");
                        }
                    } else {
                        sub_out[i] = cov;
                    }
                }
            }
        }
        return output;
    }

    Series Series::to_datetime() const {
        return cast(TimestampTypePtr);
    }

    Series Series::to_datetime(std::string const &format) const {
        return strptime(format, arrow::TimeUnit::NANO, false);
    }

    Series Series::if_else(const Series &truth_values, const Series &other) const {
        return ReturnSeriesOrThrowOnError(arrow::compute::IfElse(truth_values.m_array, m_array, other.m_array));
    }

    Series Series::if_else(const Series &truth_values, const Scalar &other) const {
        return ReturnSeriesOrThrowOnError(arrow::compute::IfElse(truth_values.m_array, m_array, other.scalar));
    }

    GenericFunctionSeriesReturn(is_finite)

    GenericFunctionSeriesReturn(is_valid)

    GenericFunctionSeriesReturn(is_null)

    GenericFunctionSeriesReturn(is_infinite)

    bool Series::is_unique() const {
        return unique().size() == size();
    }

    Series Series::where(Series const &cond, Series const &other) const {
        return if_else(cond, other);
    }

    Series Series::where(Series const &cond, Scalar const &other) const {
        return if_else(cond, other);
    }

    pd::Series Series::reindex(
            const std::shared_ptr<arrow::Array> &newIndex,
            std::optional<std::unordered_map<int64_t, int64_t>> indexer,
            const std::optional<Scalar> &fillValue) const {
        if (newIndex->type()->id() != m_index->type()->id()) {
            std::stringstream ss;
            ss << "type(NewIndex) != type(CurrentIndex).\n";
            ss << newIndex->type()->ToString() << " != " << m_index->type()->ToString() << "\nNewIndex:\n"
               << newIndex->ToString() << "\nCurrentIndex:\n"
               << m_index->ToString();

            throw std::runtime_error(ss.str());
        }

        auto new_idx_int =
                pd::ReturnOrThrowOnFailure(
                        arrow::compute::Cast(newIndex, {arrow::int64()})).array_as<arrow::Int64Array>();
        // Get the length of the new index
        int64_t newIndexLen = newIndex->length();

        // Create a new values array builder for the reindexed series
        // Build the new values array
        auto newValuesBuilder = ReturnOrThrowOnFailure(arrow::MakeBuilder(m_array->type()));

        ThrowOnFailure(newValuesBuilder->Reserve(newIndexLen));

        // Create a hashmap to store the index-value pairs of the current series
        if (not indexer) {
            indexer = std::unordered_map<int64_t, int64_t>{};
            auto idx_int =
                    pd::ReturnOrThrowOnFailure(
                            arrow::compute::Cast(m_index, {arrow::int64()})).array_as<arrow::Int64Array>();
            for (int i = 0; i < m_index->length(); i++) {
                indexer->insert_or_assign(idx_int->Value(i), i);
            }
        }

        // Iterate through the new index and add the corresponding values to the new values array builder
        for (int64_t i = 0; i < newIndexLen; i++) {
            auto &&newIndexValue = new_idx_int->Value(i);
            if (indexer->count(newIndexValue) == 1) {
                int64_t valueIndex = (*indexer)[newIndexValue];
                ThrowOnFailure(newValuesBuilder->AppendScalar(*m_array->GetScalar(valueIndex).MoveValueUnsafe()));
            } else {
                ThrowOnFailure(fillValue ? newValuesBuilder->AppendScalar(*fillValue->scalar) :
                               newValuesBuilder->AppendNull());
            }
        }

        std::shared_ptr<arrow::Array> newValues;
        ThrowOnFailure(newValuesBuilder->Finish(&newValues));

        // Return a new series with the reindexed values and new index
        return {newValues, newIndex, m_name};
    }

    pd::Series Series::reindexAsync(
            std::shared_ptr<arrow::Array> const &newIndex,
            std::optional<std::unordered_map<int64_t, int64_t>> indexer,
            const std::optional<Scalar> &fillValue) const {
        if (newIndex->type()->id() != m_index->type()->id()) {
            throw std::runtime_error(
                    "Index type of newIndex does not match "
                    "the index type of the current series.");
        }

        // Get the length of the new index
        int64_t newIndexLen = newIndex->length();
        auto new_idx_int =
                pd::ReturnOrThrowOnFailure(
                        arrow::compute::Cast(newIndex, {arrow::int64()})).array_as<arrow::Int64Array>();
        // Create a new values array builder for the reindexed series
        // Use Intel TBB to parallelize the reindex operation
        auto null = arrow::MakeNullScalar(m_array->type());
        std::vector<ScalarPtr> scalars(newIndexLen, fillValue ? fillValue->scalar : null);

        // Create a hashmap to store the index-value pairs of the current series
        if (not indexer) {
            indexer = std::unordered_map<int64_t, int64_t>{};
            auto idx_int =
                    pd::ReturnOrThrowOnFailure(
                            arrow::compute::Cast(m_index, {arrow::int64()})).array_as<arrow::Int64Array>();
            for (int i = 0; i < m_index->length(); i++) {
                indexer->insert_or_assign(idx_int->Value(i), i);
            }
        }

        tbb::parallel_for(
                0L,
                newIndexLen,
                [&](int64_t i) {
                    auto &&newIndexValue = new_idx_int->Value(i);
                    if (indexer->count(newIndexValue) == 1) {
                        int64_t valueIndex = indexer->at(newIndexValue);
                        scalars[i] = m_array->GetScalar(valueIndex).MoveValueUnsafe();
                    }
                });
        // Build the new values array
        auto newValuesBuilder = pd::ReturnOrThrowOnFailure(arrow::MakeBuilder(m_array->type()));

        ThrowOnFailure(newValuesBuilder->AppendScalars(scalars));

        std::shared_ptr<arrow::Array> newValues;
        ThrowOnFailure(newValuesBuilder->Finish(&newValues));

        // Return a new series with the reindexed values and new index
        return {newValues, newIndex, m_name};
    }

    Series Series::ReturnSeriesOrThrowOnError(arrow::Result<arrow::Datum> &&result) const {
        if (result.ok()) {
            auto arr = result->make_array();
            const int64_t arrayLength = arr->length();
            const int64_t indexLength = m_index ? m_index->length() : 0;
            if (indexLength == 0 || arrayLength == 0) {
                return pd::Series{arr, false, ""};
            } else if (arrayLength == indexLength) {
                return pd::Series{arr, m_index, ""};
            } else if (arrayLength < indexLength) {
                return pd::Series{arr, m_index->Slice(indexLength - arrayLength, arrayLength), ""};
            }
            throw std::runtime_error(
                    (boost::format(
                            "ReturnSeriesOrThrowOnError requires new Array length(%1%) <= original index Length(%2%)") %
                     arrayLength % indexLength)
                            .str());
        }
        throw std::runtime_error(result.status().ToString());
    }


    GenericFunctionSeriesReturnDateTimeLike(day)

    GenericFunctionSeriesReturnDateTimeLike(day_of_week)

    GenericFunctionSeriesReturnDateTimeLike(day_of_year)

    GenericFunctionSeriesReturnDateTimeLike(hour)

    GenericFunctionSeriesReturnDateTimeLike(is_dst)

    GenericFunctionSeriesReturnDateTimeLike(iso_week)

    GenericFunctionSeriesReturnDateTimeLike(iso_year)

    GenericFunctionSeriesReturnDateTimeLike(is_leap_year)

    GenericFunctionSeriesReturnDateTimeLike(microsecond)

    GenericFunctionSeriesReturnDateTimeLike(millisecond)

    GenericFunctionSeriesReturnDateTimeLike(month)

    GenericFunctionSeriesReturnDateTimeLike(minute)

    GenericFunctionSeriesReturnDateTimeLike(nanosecond)

    GenericFunctionSeriesReturnDateTimeLike(quarter)

    GenericFunctionSeriesReturnDateTimeLike(second)

    GenericFunctionSeriesReturnDateTimeLike(subsecond)

    GenericFunctionSeriesReturnDateTimeLike(us_week)

    GenericFunctionSeriesReturnDateTimeLike(us_year)

    GenericFunctionSeriesReturnDateTimeLike(year)

    Series
    DateTimeLike::week(bool week_starts_monday, bool count_from_zero, bool first_week_is_fully_in_year) const {
        auto opt = arrow::compute::WeekOptions{week_starts_monday, count_from_zero, first_week_is_fully_in_year};
        return ReturnSeriesOrThrowOnError(arrow::compute::CallFunction("week", {m_array}, &opt));
    }

    Series DateTimeLike::day_time_interval_between(const Series &other) const {
        return ReturnSeriesOrThrowOnError(
                arrow::compute::CallFunction("day_time_interval_between", {m_array, other.m_array}));
    }

    Series DateTimeLike::days_between(const Series &other) const {
        return ReturnSeriesOrThrowOnError(arrow::compute::CallFunction("days_between", {m_array, other.m_array}));
    }

    Series DateTimeLike::hours_between(const Series &other) const {
        return ReturnSeriesOrThrowOnError(arrow::compute::CallFunction("hours_between", {m_array, other.m_array}));
    }

    Series DateTimeLike::microseconds_between(const Series &other) const {
        return ReturnSeriesOrThrowOnError(
                arrow::compute::CallFunction("microseconds_between", {m_array, other.m_array}));
    }

    Series DateTimeLike::milliseconds_between(const Series &other) const {
        return ReturnSeriesOrThrowOnError(
                arrow::compute::CallFunction("milliseconds_between", {m_array, other.m_array}));
    }

    Series DateTimeLike::minutes_between(const Series &other) const {
        return ReturnSeriesOrThrowOnError(arrow::compute::CallFunction("minutes_between", {m_array, other.m_array}));
    }

    Series DateTimeLike::month_day_nano_interval_between(const Series &other) const {
        return ReturnSeriesOrThrowOnError(
                arrow::compute::CallFunction("month_day_nano_interval_between", {m_array, other.m_array}));
    }

    Series DateTimeLike::month_interval_between(const Series &other) const {
        return ReturnSeriesOrThrowOnError(
                arrow::compute::CallFunction("month_interval_between", {m_array, other.m_array}));
    }

    Series DateTimeLike::nanoseconds_between(const Series &other) const {
        return ReturnSeriesOrThrowOnError(
                arrow::compute::CallFunction("nanoseconds_between", {m_array, other.m_array}));
    }

    Series DateTimeLike::quarters_between(const Series &other) const {
        return ReturnSeriesOrThrowOnError(arrow::compute::CallFunction("quarters_between", {m_array, other.m_array}));
    }

    Series DateTimeLike::seconds_between(const Series &other) const {
        return ReturnSeriesOrThrowOnError(arrow::compute::CallFunction("seconds_between", {m_array, other.m_array}));
    }

    Series DateTimeLike::weeks_between(const Series &other) const {
        return ReturnSeriesOrThrowOnError(arrow::compute::CallFunction("weeks_between", {m_array, other.m_array}));
    }

    Series DateTimeLike::years_between(const Series &other) const {
        return ReturnSeriesOrThrowOnError(arrow::compute::CallFunction("years_between", {m_array, other.m_array}));
    }

} // namespace pd