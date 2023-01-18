//
// Created by dewe on 12/28/22.
//
#include "series.h"
#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/compute/exec/aggregate.h>
#include <arrow/io/api.h>
#include <tabulate/table.hpp>
#include <unordered_set>
#include "arrow/compute/kernels/autocorr.h"
#include "arrow/compute/kernels/corr.h"
#include "arrow/compute/kernels/cov.h"
#include "arrow/compute/kernels/cumprod.h"
#include "arrow/compute/kernels/pct_change.h"
#include "datetimelike.h"
#include "filesystem"
#include "ranges"
#include "stringlike.h"


#define BINARY_OPERATOR(sign, name) \
Series Series:: operator sign (const Series &a) const { \
    return ValidateHelper(arrow::compute::CallFunction(#name, {m_array, a.m_array})); \
}                     \
\
Series Series:: operator sign (const Scalar &a) const{ \
    return ValidateHelper(arrow::compute::CallFunction(#name, {m_array, a.scalar})); \
}                     \
\
Series operator sign (Scalar const& a, Series const& b) { \
  return ValidateHelper(arrow::compute::CallFunction(#name, {a.value(), b.m_array})); \
}

#define GenericFunction(name, ReturnFilter, OutT, ClassT) \
OutT ClassT:: name() const {                            \
    auto result = arrow::compute::CallFunction( #name, {m_array});   \
    if(result.ok())                               \
    {   \
        return ReturnFilter;   \
    }                                             \
    else                                          \
    {  \
        throw std::runtime_error(result.status().ToString());   \
        }   \
}

#define GenericFunctionScalarReturn(name) GenericFunction(name, result->scalar()->shared_from_this(), Scalar, Series)
#define GenericFunctionSeriesReturn(name) GenericFunction(name, (Series{result->make_array(), false, m_name}), Series, Series)

#define GenericFunctionSeriesReturnString(name) GenericFunction(name, (Series{result->make_array(), false, ""}), Series, StringLike)
#define GenericFunctionScalarReturnString(name) GenericFunction(name, result->scalar()->shared_from_this(), Scalar, StringLike)

#define GenericFunctionScalarReturnDateTimeLike(name) GenericFunction(name, result->scalar()->shared_from_this(), Scalar, DateTimeLike)
#define GenericFunctionSeriesReturnDateTimeLike(name) GenericFunction(name, (Series{result->make_array(), false, ""}), Series, DateTimeLike)

#define GenericFunctionSeriesReturnRename(name, f_name, ClassT) \
Series ClassT :: name () const{ \
     return ValidateHelper(arrow::compute::CallFunction(#f_name, {m_array}));                                                    \
}

#define Aggregation(name, ReturnT) \
ReturnT Series:: name (bool skip_null) const {    \
arrow::compute::ScalarAggregateOptions opt{skip_null};                                   \
auto result = arrow::compute::CallFunction(# name, {m_array}, &opt);   \
if(result.ok()) \
{   \
    return result->scalar_as< typename arrow::CTypeTraits<ReturnT>::ScalarType>().value;   \
}else{  \
throw std::runtime_error(result.status().ToString());   \
}   \
}

#define AggregationWithCustomOption(name, f_name, ReturnT, option) \
ReturnT Series:: name () const {    \
auto opt = option;                                                                  \
auto result = arrow::compute::CallFunction( #f_name, {m_array}, &opt);   \
if(result.ok()) \
{   \
    return result->scalar_as< typename arrow::CTypeTraits<ReturnT>::ScalarType>().value;   \
}else{  \
throw std::runtime_error(result.status().ToString());   \
}   \
}

namespace pd
{
    Series::Series(std::shared_ptr<arrow::Array> const& arr, bool isIndex,
                   std::string name):
            NDArray<Series>(arr, nullptr, true),
            m_name(std::move(name)){

        if(isIndex)
        {
            setIndexer();
        }
        else
        {
            m_index = uint_range(arr->length());
        }
    }

    Series::Series(std::shared_ptr<arrow::Array> const& arr,
                   std::shared_ptr<arrow::Array> const& index,
                   std::string name,
                   bool skipIndex):
            NDArray<Series>(arr, index, skipIndex),
            m_name(std::move(name)){}

    std::shared_ptr<arrow::DataType> Series::dtype() const
    {
        return m_array->type();
    }

    uint64_t Series::nbytes() const
    {
        return m_array->data()->buffers[1]->size();
    }

    int64_t Series::size() const
    {
        return m_array->length();
    }

    bool Series::empty() const
    {
        return m_array->length() == 0;
    }

    void Series::add_prefix(std::string prefix)
    {
        m_name = prefix.append(m_name);
    }

    void Series::add_suffix(std::string const &suffix)
    {
        m_name.append(suffix);
    }

    void Series::rename(std::string const &newName)
    {
        m_name = newName;
    }

    std::string Series::name() const
    {
        return m_name;
    }

    Scalar Series::operator[](int index) const
    {
        auto&& result =
            m_array->GetScalar(index >= 0 ? index : m_array->length() + index);

        if (result.ok())
        {
            return std::move(result.ValueUnsafe());
        }
        else
        {
            throw std::runtime_error(result.status().ToString());
        }
    }

    Series Series::operator[](Slice  slice) const
    {
        slice.normalize(size());
        return { slice.end == 0 ?
                     m_array->Slice(slice.start) :
                     m_array->Slice(slice.start, slice.end - slice.start),
                 false,
                 m_name };
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

    Series Series::operator-()  const{
        return ValidateHelper(arrow::compute::Negate(m_array, arrow::compute::ArithmeticOptions{false}));
    }

    GenericFunctionScalarReturn(min)
    GenericFunctionScalarReturn(max)
    GenericFunctionScalarReturn(product)
    GenericFunctionScalarReturn(sum)

    Scalar Series::agg(std::string const& name,
                       bool skip_null)  const{
        arrow::compute::ScalarAggregateOptions opt{skip_null};
        auto result = arrow::compute::CallFunction(name, {m_array}, &opt);
        if (result.ok()) { return std::move(result->scalar()->shared_from_this()); }
        else {
            throw std::runtime_error(result.status().ToString());
        }
    }

    Scalar Series::quantile(float q)  const{
        arrow::compute::QuantileOptions opt{q};
        auto result = arrow::compute::CallFunction("quantile", {m_array}, &opt);
        if (result.ok()) { return std::move(result->scalar()->shared_from_this()); }
        else {
            throw std::runtime_error(result.status().ToString());
        }
    }

    Scalar Series::tdigest(float q)  const{
        arrow::compute::QuantileOptions opt{q};
        auto result = arrow::compute::CallFunction("tdigest", {m_array}, &opt);
        if (result.ok()) { return std::move(result->scalar()->shared_from_this()); }
        else {
            throw std::runtime_error(result.status().ToString());
        }
    }

    MinMax Series::min_max(bool skip_null) const
    {
        auto result = arrow::compute::MinMax(m_array,
                                             arrow::compute::ScalarAggregateOptions{skip_null});
        if(result.ok())
        {
            auto minmax = result->scalar_as<arrow::StructScalar>().value;
            return {std::move(minmax[0]),std::move(minmax[1])};
        }else{
            throw std::runtime_error(result.status().ToString());
        }
    }

    Series Series::mode(int n, bool skip_nulls)  const{
        return ValidateHelper(arrow::compute::Mode(m_array,
                                                   arrow::compute::ModeOptions{n, skip_nulls}));
    }

    Aggregation(all, bool)
    Aggregation(any, bool)
    Aggregation(mean, double)

    AggregationWithCustomOption(count, count, int64_t,
                                arrow::compute::CountOptions{})

    AggregationWithCustomOption(count_na, count, int64_t,
                                arrow::compute::CountOptions{
        arrow::compute::CountOptions::CountMode::ONLY_NULL})

    AggregationWithCustomOption(nunique, count_distinct, int64_t,
                                arrow::compute::CountOptions{})

    double Series::std(int ddof, bool skip_na)  const {
        auto result = arrow::compute::Stddev(m_array,
                                             arrow::compute::VarianceOptions{ddof, skip_na});
        if(result.ok())
        {
            return result->scalar_as<arrow::DoubleScalar>().value;
        }else{
            throw std::runtime_error(result.status().ToString());
        }
    }

    int64_t Series::index(Scalar const& search)  const
    {
        if (isIndex)
        {
            try{
                return indexer.at(search.scalar);
            }
            catch (std::out_of_range const& )
            {
                return -1;
            }
        }
        else
        {
            auto result = arrow::compute::Index(
                m_array,
                arrow::compute::IndexOptions{ search.value() });
            if (result.ok())
            {
                return result->scalar_as<arrow::Int64Scalar>().value;
            }
            else
            {
                throw std::runtime_error(result.status().ToString());
            }
        }
    }

    double Series::median(bool skip_null) const {
        arrow::compute::ScalarAggregateOptions opt{skip_null};
        auto result = arrow::compute::CallFunction("approximate_median",
                                                   {m_array}, &opt);
        if(result.ok())
        {
            return result->scalar_as<arrow::DoubleScalar>().value;
        }else{
            throw std::runtime_error(result.status().ToString());
        }
    }

    double Series::var(int ddof, bool skip_na)  const {
        return ValidateHelperScalar<double>(
                arrow::compute::Variance(m_array,
                                         arrow::compute::VarianceOptions{ddof, skip_na}));
    }

    Series Series::pow(double x)  const{
        auto result = arrow::compute::Power({m_array}, arrow::MakeScalar(x));
        if (result.ok()) {
            return {result->make_array(), false, m_name};
        } else {
            throw std::runtime_error(result.status().ToString());
        }
    }

    GenericFunctionSeriesReturn(abs)
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
        return ValidateHelper(arrow::compute::RoundToMultiple({m_array}, opt));
    }

    Series Series::logb(int base)  const{
        return ValidateHelper(arrow::compute::Logb({m_array}, arrow::MakeScalar(base)));
    }

    GenericFunctionSeriesReturnRename(operator~, bit_wise_not, Series)
    GenericFunctionSeriesReturnRename(operator!, invert, Series)

    Series Series::cumsum(double start, bool skip_nulls) const
    {
        return ValidateHelper(arrow::compute::CumulativeSum(
            m_array,
            arrow::compute::CumulativeSumOptions{ start, skip_nulls }));
    }

    Series Series::cumprod(double start, bool skip_nulls)  const
    {
        return ValidateHelper(arrow::compute::CumulativeProduct(
            m_array,
            arrow::compute::CumulativeProductOptions{ start, skip_nulls }));
    }

    std::shared_ptr<arrow::DictionaryArray> Series::dictionary_encode()  const{

        auto result = arrow::compute::DictionaryEncode(m_array);
        if (result.ok()) {
            return result.MoveValueUnsafe().array_as<arrow::DictionaryArray>();
        } else {
            throw std::runtime_error(result.status().ToString());
        }
    }

    GenericFunctionSeriesReturn(unique)
    GenericFunctionSeriesReturnRename(drop_na, drop_null, Series)
    GenericFunctionSeriesReturn(indices_nonzero)

    Series Series::where(const Series & _filter) const {
        arrow::compute::FilterOptions opt{
                arrow::compute::FilterOptions::NullSelectionBehavior::EMIT_NULL};
        return ValidateHelper(arrow::compute::CallFunction("array_filter", {m_array, _filter.m_array}, &opt));
    }

    Series Series::take(const Series & x)  const{
        if(x.dtype()->id() == arrow::Type::BOOL)
        {
            return ValidateHelper(arrow::compute::CallFunction("array_filter", {m_array, x.m_array}));
        }
        else
        {
            return ValidateHelper(arrow::compute::CallFunction("array_take", {m_array, x.m_array}));
        }
    }

    GenericFunctionSeriesReturnString(ascii_capitalize)
    GenericFunctionSeriesReturnString(ascii_lower)
    GenericFunctionSeriesReturnString(ascii_reverse)
    GenericFunctionSeriesReturnString(ascii_swapcase)
    GenericFunctionSeriesReturnString(ascii_title)
    GenericFunctionSeriesReturnString(ascii_upper)

    GenericFunctionScalarReturnString(binary_length)
    GenericFunctionSeriesReturnString(binary_repeat)
    GenericFunctionSeriesReturnString(binary_reverse)

    GenericFunctionSeriesReturnString(utf8_capitalize)
    GenericFunctionSeriesReturnString(utf8_length)
    GenericFunctionSeriesReturnString(utf8_lower)
    GenericFunctionSeriesReturnString(utf8_reverse)
    GenericFunctionSeriesReturnString(utf8_swapcase)
    GenericFunctionSeriesReturnString(utf8_title)
    GenericFunctionSeriesReturnString(utf8_upper)

    GenericFunctionSeriesReturnString(ascii_is_alnum)
    GenericFunctionSeriesReturnString(ascii_is_alpha)
    GenericFunctionSeriesReturnString(ascii_is_decimal)
    GenericFunctionSeriesReturnString(ascii_is_lower)
    GenericFunctionSeriesReturnString(utf8_is_alnum)
    GenericFunctionSeriesReturnString(utf8_is_alpha)
    GenericFunctionSeriesReturnString(utf8_is_decimal)
    GenericFunctionSeriesReturnString(utf8_is_digit)
    GenericFunctionSeriesReturnString(utf8_is_lower)
    GenericFunctionSeriesReturnString(utf8_is_numeric)
    GenericFunctionSeriesReturnString(utf8_is_printable)
    GenericFunctionSeriesReturnString(utf8_is_space)
    GenericFunctionSeriesReturnString(utf8_is_upper)

    GenericFunctionSeriesReturnString(ascii_is_title)
    GenericFunctionSeriesReturnString(utf8_is_title)
    GenericFunctionSeriesReturnString(string_is_ascii)

    GenericFunctionSeriesReturnString(ascii_ltrim_whitespace)
    GenericFunctionSeriesReturnString(ascii_rtrim_whitespace)
    GenericFunctionSeriesReturnString(ascii_trim_whitespace)
    GenericFunctionSeriesReturnString(utf8_ltrim_whitespace)
    GenericFunctionSeriesReturnString(utf8_rtrim_whitespace)
    GenericFunctionSeriesReturnString(utf8_trim_whitespace)

    StringLike Series::str() const
    {
        if (arrow::is_binary_like(m_array->type_id()))
        {
            return { m_array };
        }
        return { this->cast(std::make_shared<arrow::StringType>()).m_array };
    }

    DateTimeLike Series::dt() const
    {
        if (arrow::is_temporal(m_array->type_id()))
        {
            return { m_array };
        }
        return { this->cast(std::make_shared<arrow::TimestampType>(
                                arrow::TimeUnit::NANO))
                     .m_array };
    }

    Series StringLike::binary_replace_slice(int64_t start,
                                            int64_t stop,
                                            std::string const& replacement) const
    {
        arrow::compute::ReplaceSliceOptions opt(start, stop, replacement);
        return ValidateHelper(arrow::compute::CallFunction("binary_replace_slice",
                                                           {m_array}, &opt));
    }

    Series StringLike::replace_substring(std::string const& pattern,
                                         std::string const& replacement,
                                         int64_t const& max_replacements) const
    {
        arrow::compute::ReplaceSubstringOptions opt(pattern, replacement, max_replacements);
        return ValidateHelper(arrow::compute::CallFunction("replace_substring",
                                                           {m_array}, &opt));
    }

    Series StringLike::replace_substring_regex(std::string const& pattern,
                                               std::string const& replacement,
                                               int64_t const& max_replacements) const
    {
        arrow::compute::ReplaceSubstringOptions opt(pattern, replacement, max_replacements);
        return ValidateHelper(arrow::compute::CallFunction("replace_substring_regex",
                                                           {m_array}, &opt));
    }

    Series StringLike::utf8_replace_slice(int64_t start, int64_t stop, std::string const& replacement) const
    {
        arrow::compute::ReplaceSliceOptions opt(start, stop, replacement);
        return ValidateHelper(arrow::compute::CallFunction("utf8_replace_slice",
                                                           {m_array}, &opt));
    }

    Series StringLike::ascii_center(int64_t width, std::string const& padding) const
    {
        arrow::compute::PadOptions opt(width, padding);
        return ValidateHelper(arrow::compute::CallFunction("ascii_center",
                                                           {m_array}, &opt));
    }

    Series StringLike::ascii_lpad(int64_t width, std::string const& padding) const
    {
        arrow::compute::PadOptions opt(width, padding);
        return ValidateHelper(arrow::compute::CallFunction("ascii_lpad",
                                                           {m_array}, &opt));
    }

    Series StringLike::ascii_rpad(int64_t width, std::string const& padding) const
    {
        arrow::compute::PadOptions opt(width, padding);
        return ValidateHelper(arrow::compute::CallFunction("ascii_rpad",
                                                           {m_array}, &opt));
    }

    Series StringLike::utf8_center(int64_t width, std::string const& padding) const
    {
        arrow::compute::PadOptions opt(width, padding);
        return ValidateHelper(arrow::compute::CallFunction("utf8_center",
                                                           {m_array}, &opt));
    }

    Series StringLike::utf8_lpad(int64_t width, std::string const& padding) const
    {
        arrow::compute::PadOptions opt(width, padding);
        return ValidateHelper(arrow::compute::CallFunction("utf8_lpad",
                                                           {m_array}, &opt));
    }

    Series StringLike::utf8_rpad(int64_t width, std::string const& padding) const
    {
        arrow::compute::PadOptions opt(width, padding);
        return ValidateHelper(arrow::compute::CallFunction("utf8_rpad",
                                                           {m_array}, &opt));
    }

    Series StringLike::ascii_split_whitespace(int64_t max_splits, bool reverse) const
    {
        arrow::compute::SplitOptions opt(max_splits, reverse);
        return ValidateHelper(arrow::compute::CallFunction(
            "ascii_split_whitespace",
            { m_array },
            &opt));
    }

    Series StringLike::utf8_split_whitespace(int64_t max_splits, bool reverse) const {
        arrow::compute::SplitOptions opt(max_splits, reverse);
        return ValidateHelper(arrow::compute::CallFunction("utf8_split_whitespace",
                                                           {m_array}, &opt));
    }

    Series StringLike::split_pattern(std::string const& pattern, int64_t max_splits, bool reverse) const {
        arrow::compute::SplitPatternOptions opt(pattern, max_splits, reverse);
        return ValidateHelper(arrow::compute::CallFunction("split_pattern",
                                                           {m_array}, &opt));
    }

    Series StringLike::split_pattern_regex(std::string const& pattern, int64_t max_splits, bool reverse) const {
        arrow::compute::SplitPatternOptions opt(pattern, max_splits, reverse);
        return ValidateHelper(arrow::compute::CallFunction("split_pattern_regex",
                                                           {m_array}, &opt));
    }

    Series StringLike::ascii_ltrim(const std::string &characters) const {
        arrow::compute::TrimOptions opt(characters);
        return ValidateHelper(arrow::compute::CallFunction("ascii_ltrim",
                                                           {m_array}, &opt));
    }

    Series StringLike::ascii_rtrim(const std::string &characters) const {
        arrow::compute::TrimOptions opt(characters);
        return ValidateHelper(arrow::compute::CallFunction("ascii_rtrim",
                                                           {m_array}, &opt));
    }

    Series StringLike::ascii_trim(const std::string &characters) const {
        arrow::compute::TrimOptions opt(characters);
        return ValidateHelper(arrow::compute::CallFunction("ascii_trim",
                                                           {m_array}, &opt));
    }

    Series StringLike::utf8_ltrim(const std::string &characters) const {
        arrow::compute::TrimOptions opt(characters);
        return ValidateHelper(arrow::compute::CallFunction("utf8_ltrim",
                                                           {m_array}, &opt));
    }

    Series StringLike::utf8_rtrim(const std::string &characters) const {
        arrow::compute::TrimOptions opt(characters);
        return ValidateHelper(arrow::compute::CallFunction("utf8_rtrim",
                                                           {m_array}, &opt));
    }

    Series StringLike::utf8_trim(const std::string &characters) const {
        arrow::compute::TrimOptions opt(characters);
        return ValidateHelper(arrow::compute::CallFunction("utf8_trim",
                                                           {m_array}, &opt));
    }

    Series StringLike::extract_regex(const std::string &pattern) const {
        arrow::compute::ExtractRegexOptions opt(pattern);
        return ValidateHelper(arrow::compute::CallFunction("extract_regex",
                                                           {m_array}, &opt));
    }

    Series StringLike::binary_join(const Series &joiner) const {
        return ValidateHelper(arrow::compute::CallFunction("binary_join", {m_array, joiner.m_array}));
    }

    Series StringLike::binary_join(const Scalar &joiner) const {
        return ValidateHelper(arrow::compute::CallFunction("binary_join", {m_array, joiner.scalar}));
    }

    Series StringLike::utf8_slice_codeunits(int64_t start, int64_t stop, int64_t step) const {
        arrow::compute::SliceOptions opt(start, stop, step);
        return ValidateHelper(arrow::compute::CallFunction("utf8_slice_codeunits", {m_array}, &opt));
    }

    Series StringLike::count_substring(const std::string &pattern, bool ignore_case) {
        arrow::compute::MatchSubstringOptions opt(pattern, ignore_case);
        return ValidateHelper(arrow::compute::CallFunction("count_substring", {m_array}, &opt));
    }

    Series StringLike::count_substring_regex(const std::string &pattern, bool ignore_case) {
        arrow::compute::MatchSubstringOptions opt(pattern, ignore_case);
        return ValidateHelper(arrow::compute::CallFunction("count_substring_regex", {m_array}, &opt));
    }

    Series StringLike::ends_with(const std::string &pattern, bool ignore_case) {
        arrow::compute::MatchSubstringOptions opt(pattern, ignore_case);
        return ValidateHelper(arrow::compute::CallFunction("ends_with", {m_array}, &opt));
    }

    Series StringLike::find_substring(const std::string &pattern, bool ignore_case) {
        arrow::compute::MatchSubstringOptions opt(pattern, ignore_case);
        return ValidateHelper(arrow::compute::CallFunction("find_substring", {m_array}, &opt));
    }

    Series StringLike::match_like(const std::string &pattern, bool ignore_case) {
        arrow::compute::MatchSubstringOptions opt(pattern, ignore_case);
        return ValidateHelper(arrow::compute::CallFunction("match_like", {m_array}, &opt));
    }

    Series StringLike::match_substring_regex(const std::string &pattern, bool ignore_case) {
        arrow::compute::MatchSubstringOptions opt(pattern, ignore_case);
        return ValidateHelper(arrow::compute::CallFunction("match_substring_regex", {m_array}, &opt));
    }

    Series StringLike::match_substring(const std::string &pattern, bool ignore_case) {
        arrow::compute::MatchSubstringOptions opt(pattern, ignore_case);
        return ValidateHelper(arrow::compute::CallFunction("match_substring", {m_array}, &opt));
    }

    Series StringLike::starts_with(const std::string &pattern, bool ignore_case) {
        arrow::compute::MatchSubstringOptions opt(pattern, ignore_case);
        return ValidateHelper(arrow::compute::CallFunction("starts_with", {m_array}, &opt));
    }

    Series StringLike::index_in(const Series &value_set, bool skip_nulls) {
        arrow::compute::SetLookupOptions opt(value_set.array(), skip_nulls);
        return ValidateHelper(arrow::compute::CallFunction("index_in", {m_array}, &opt));
    }

    Series StringLike::is_in(const Series &value_set, bool skip_nulls) {
        arrow::compute::SetLookupOptions opt(value_set.array(), skip_nulls);
        return ValidateHelper(arrow::compute::CallFunction("is_in", {m_array}, &opt));
    }

    Series DateTimeLike::ceil(int multiple, arrow::compute::CalendarUnit unit, bool week_starts_monday,
                              bool ceil_is_strictly_greater, bool calendar_based_origin) const {
        arrow::compute::RoundTemporalOptions opt(multiple, unit, week_starts_monday,
                                                 ceil_is_strictly_greater, calendar_based_origin);
        return ValidateHelper(arrow::compute::CallFunction("ceil_temporal", {m_array}, &opt));
    }

    Series DateTimeLike::floor(int multiple, arrow::compute::CalendarUnit unit, bool week_starts_monday,
                              bool ceil_is_strictly_greater, bool calendar_based_origin) const {
        arrow::compute::RoundTemporalOptions opt(multiple, unit, week_starts_monday,
                                                 ceil_is_strictly_greater, calendar_based_origin);
        return ValidateHelper(arrow::compute::CallFunction("floor_temporal", {m_array}, &opt));
    }

    Series DateTimeLike::round(int multiple, arrow::compute::CalendarUnit unit, bool week_starts_monday,
                              bool ceil_is_strictly_greater, bool calendar_based_origin) const {
        arrow::compute::RoundTemporalOptions opt(multiple, unit, week_starts_monday,
                                                 ceil_is_strictly_greater, calendar_based_origin);
        return ValidateHelper(arrow::compute::CallFunction("round_temporal", {m_array}, &opt));
    }

    Series Series::cast(const std::shared_ptr<arrow::DataType> &dt, bool safe) const
    {
        return ValidateHelper(arrow::compute::Cast(m_array, dt,
                                                   arrow::compute::CastOptions{safe}) );
    }

    Series Series::strftime(const std::string &format, std::string const& locale)  const {
        return ValidateHelper(arrow::compute::Strftime(m_array,
                                                       arrow::compute::StrftimeOptions{format, locale}));
    }

    Series Series::strptime(const std::string &format,
                            arrow::TimeUnit::type unit,
                            bool error_is_null)  const {
        return ValidateHelper(arrow::compute::Strptime(m_array,
                                                       arrow::compute::StrptimeOptions{format, unit, error_is_null}));
    }


    Series Series::shift(int32_t shift_value,
                         std::shared_ptr<arrow::Scalar> const& fill_value)  const
    {
        return ValidateHelper(arrow::compute::Shift(
            m_array,
            arrow::compute::ShiftOptions(shift_value, fill_value)));
    }

    bool Series::is_valid(int row) const
    {
        return pd::ValidateAndReturn(m_array->GetScalar(row))->is_valid;
    }

    Series Series::pct_change(int64_t periods)  const
    {
        return ValidateHelper(arrow::compute::PctChange(m_array, periods));
    }

    GenericFunctionSeriesReturnRename(ffill, fill_null_forward, Series)
    GenericFunctionSeriesReturnRename(bfill, fill_null_backward, Series)

    Series Series::replace_with_mask(Series const& cond, Series const& other)  const{
        if( (cond.size() == other.size()) and (other.size() <= this->size()) )
        {
            return ValidateHelper(arrow::compute::ReplaceWithMask(m_array,
                                                                  cond.m_array,
                                                                  other.m_array));
        }
        else
        {
            throw std::runtime_error(""
                "replace_with_mask error: valid precondition "
                "(cond.size() == other.size()) and (other.size() <= this->size())");
        }

    }

    Series Series::intersection(Series const& other) const
    {
        if (!isIndex || !other.isIndex)
        {
            throw std::runtime_error(
                "Both Series must be indexes for intersection to be valid.");
        }

        arrow::Int64Builder intersection_indices;
        std::vector<int64_t> result;
        result.reserve(indexer.size());
        for (auto const& [index, value] : indexer)
        {
            if (other.indexer.count(index) == 1)
            {
                result.push_back(value);
            }
        }
        std::sort(result.begin(), result.end());
        throwOnNotOkStatus(intersection_indices.AppendValues(result));

        auto intersection_array = pd::ValidateAndReturn(arrow::compute::Take(
            m_array,
            intersection_indices.Finish().MoveValueUnsafe()));
        return { intersection_array.make_array(), true };
    }

    Series Series::union_(const Series &other) const
    {
        if (!isIndex || !other.isIndex)
        {
            throw std::runtime_error(
                "Both Series objects must be indexes to perform union.");
        }

        if (!isIndex || !other.isIndex)
        {
            throw std::runtime_error(
                "Both Series must be indexes to perform union operation.");
        }

        auto concatenated_arrays = pd::ValidateAndReturn(
            arrow::Concatenate({ m_array, other.m_array }));

        auto new_series = Series(
            pd::ValidateAndReturn(arrow::compute::Unique(concatenated_arrays)),
            nullptr,
            m_name,
            true);
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

    std::vector<std::shared_ptr<arrow::Scalar>>
    Series::get_indexed_values() const {
        if (!isIndex) {
            throw std::runtime_error("Cannot get indexed values from non-index series");
        }
        auto key_view = indexer | std::views::keys;
        return {key_view.begin(), key_view.end()};
    }

    Series Series::append(const Series &to_append,
                          bool ignore_index)  const
    {
        if (!ignore_index)
        {
            if (isIndex != to_append.isIndex)
            {
                throw std::runtime_error(
                    "Index type must be the same for both series.");
            }
            if (isIndex && to_append.isIndex)
            {
                if(intersection(to_append).size() > 0)
                {
                    throw std::runtime_error(
                        "Index must be the unique for both series if series is an Index.");
                }
            }
            if (not isIndex)
            {
                if ((m_index == nullptr && to_append.m_index != nullptr) ||
                    (m_index != nullptr && to_append.m_index == nullptr))
                {
                    throw std::runtime_error(
                        "If ignore_index is not set both indexes must either have values or both be null");
                }
                else if (m_index == nullptr && to_append.m_index == nullptr)
                {
                    ignore_index = true;
                }
            }

            auto concatenated_arrays =
                arrow::Concatenate({ m_array, to_append.m_array })
                    .MoveValueUnsafe();

            if (isIndex)
            {
                Indexer new_indexer;
                int i = 0;
                for(i = 0; i < concatenated_arrays->length(); i++)
                {
                    new_indexer[concatenated_arrays->GetScalar(i)
                                    .MoveValueUnsafe()] = i;
                }

                auto new_series =
                    Series(concatenated_arrays, nullptr, m_name, true);
                new_series.indexer = new_indexer;
                new_series.isIndex = true;

                return new_series;
            }
            else
            {
                return Series(
                    concatenated_arrays,
                    arrow::Concatenate({ m_index, to_append.m_index })
                        .MoveValueUnsafe(),
                    m_name);
            }
        }
        else
        {
            auto concatenated_arrays =
                arrow::Concatenate({ m_array, to_append.m_array })
                    .MoveValueUnsafe();
            return Series(concatenated_arrays, nullptr, m_name);
        }
    }

    Series Series::argsort(bool ascending)  const{
        auto opt = arrow::compute::ArraySortOptions{
            ascending ? arrow::compute::SortOrder::Ascending :
            arrow::compute::SortOrder::Descending};
        return ValidateHelper(
                arrow::compute::CallFunction("array_sort_indices", {m_array}, &opt)
                );
    }

    double Series::cov(const Series &s2)  const{
        return ValidateHelperScalar<double>(arrow::compute::Covariance(
            m_array,
            s2.m_array,
            arrow::compute::VarianceOptions(1)));
    }

    double Series::corr(const Series &s2, CorrelationType method) const
    {
        switch (method)
        {
            case CorrelationType::Pearson:
                return ValidateHelperScalar<double>(arrow::Correlation(
                    m_array,
                    s2.m_array,
                    arrow::compute::VarianceOptions(1)));
            case CorrelationType::Kendall:
            case CorrelationType::Spearman:
                throw std::runtime_error(
                    "Series currently only supports Pearson CorrelationType");
        }
        return 0;
    }

    double Series::corr(const Series &s2, double (*method)(double)) const {
        throw std::runtime_error("Series currently only supports Pearson CorrelationType");
    }

    std::ostream & operator<<(std::ostream &os, Series const &series)
    {
        auto N = int(series.size());
        int maxDigit = 0;

        while (N > 0)
        {
            N = N / 10;
            maxDigit++;
        }
        for (int i = 0; i < series.size(); i++)
        {
            auto index_status = (series.m_index==nullptr) ? arrow::MakeScalar(i) :
                                                   series.m_index->GetScalar(i);
            if (index_status.ok())
                os << std::setw(maxDigit)
                   << index_status.MoveValueUnsafe()->ToString() << "\t"
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
    Series::ewm(double value,
                EWMAlphaType type,
                bool adjust,
                bool ignore_na,
                int min_periods) const
    {
        min_periods = std::max(min_periods, 1);
        double comass{};
        switch (type)
        {
            case EWMAlphaType::CenterOfMass:
                if (value >= 0)
                {
                    comass = value;
                }
                else
                {
                    throw std::runtime_error("EWM with COM require Value >= 0");
                }
                break;
            case EWMAlphaType::Span:
                if (value >= 1)
                {
                    comass = (value - 1) / 2;
                }
                else
                {
                    throw std::runtime_error(
                        "EWM with Span require Value >= 1");
                }
                break;
            case EWMAlphaType::HalfLife:
                if (value > 0)
                {
                    double decay = 1 - std::exp(std::log(0.5) / value);
                    comass = 1 / decay - 1;
                }
                    throw std::runtime_error(
                        "EWM is not supported");
                break;
            case EWMAlphaType::Alpha:
                if (value > 0 and value <= 1)
                {
                    comass = (1 - value) / value;
                }
                else
                {
                    throw std::runtime_error(
                        "EWM with Alpha require 0 < value <= 1");
                }
                break;
        };

        auto casted =
            ValidateAndReturn(arrow::compute::Cast(m_array, DoubleTypePtr))
                .make_array();
        auto doubleArray =
            arrow::internal::checked_pointer_cast<arrow::DoubleArray>(casted);

        return { ewm(doubleArray->begin(),
                     doubleArray->length(),
                     min_periods,
                     comass,
                     adjust,
                     ignore_na,
                     {}),
                 m_name,
                 m_index };
    }

    Series Series::nth_element(int n) const {
        auto opt = arrow::compute::PartitionNthOptions{n};
        return ValidateHelper(arrow::compute::CallFunction("partition_nth_indices", {m_array}, &opt));
    }

    std::array<std::shared_ptr<arrow::Array>, 2>
    Series::sort(bool ascending) const {
        auto opt = arrow::compute::ArraySortOptions{
                ascending ? arrow::compute::SortOrder::Ascending :
                arrow::compute::SortOrder::Descending};
        auto result = arrow::compute::CallFunction("array_sort_indices", {m_array}, &opt);
        if(result.ok())
        {
            auto indices = result.MoveValueUnsafe();
            return {arrow::compute::Take(m_array, indices)->make_array(), indices.make_array()};
        }
        throw std::runtime_error(result.status().ToString());
    }

    Series Series::operator[](const StringSlice & slicer) const {
        if(m_index->type_id() == arrow::Type::STRING)
        {
            int64_t start = 0, end = m_index->length();
            if(slicer.start)
            {
                start = ValidateHelperScalar<int64_t>(
                        arrow::compute::Index(m_index,
                                              arrow::compute::IndexOptions{arrow::MakeScalar(slicer.start.value())} ) );
                if(start == -1)
                {
                    throw std::runtime_error("invalid start index");
                }
            }
            if(slicer.end) {
                end = ValidateHelperScalar<int64_t>(
                        arrow::compute::Index(m_index,
                                              arrow::compute::IndexOptions{arrow::MakeScalar(slicer.end.value())}));
                if(end == -1)
                {
                    throw std::runtime_error("invalid end index");
                }
            }
            return operator[]({start, end});
        }else
        {
            std::stringstream ss;
            ss << "Type Error: String slicing is only allowed on STRING DataType, but found index of type "
               << m_index->type()->ToString() << "\n";
            throw std::runtime_error(ss.str());
        }
    }

    Series Series::operator[](const DateTimeSlice & slicer) const {
        if(m_index->type_id() == arrow::Type::TIMESTAMP)
        {
            int64_t start = 0, end = m_index->length();

            if(slicer.start)
            {
                start = ValidateHelperScalar<int64_t>(
                        arrow::compute::Index(m_index,
                                              arrow::compute::IndexOptions{fromDateTime(slicer.start.value())} ) );
                if(start == -1)
                {
                    throw std::runtime_error("invalid start index");
                }
            }
            if(slicer.end) {
                end = ValidateHelperScalar<int64_t>(
                        arrow::compute::Index(m_index,
                                              arrow::compute::IndexOptions{fromDateTime(slicer.end.value())}));
                if(end == -1)
                {
                    throw std::runtime_error("invalid end index");
                }
            }
            return operator[]({start, end});
        }else
        {
            std::stringstream ss;
            ss << "Type Error: DateTime slicing is only allowed on TimeStamp DataType, but found index of type "
            << m_index->type()->ToString() << "\n";
            throw std::runtime_error(ss.str());
        }
    }

    vector<double> Series::ewm(
        const arrow::DoubleArray::IteratorType& vals,
        size_t N,
        int minp,
        double com,
        bool adjust,
        bool ignore_na,
        const vector<double>& deltas,
        bool normalize)
    {
        vector<double> output(N);
        if (N == 0)
        {
            return output;
        }
        bool use_deltas = !deltas.empty();
        double alpha = 1. / (1. + com);
        double old_wt_factor = 1. - alpha;
        double new_wt = adjust ? 1. : alpha;

        std::optional<double> weighted = vals[0];
        bool is_observation = weighted.has_value();
        int nobs = (is_observation);
        output[0] = (nobs >= minp and weighted.has_value()) ? *weighted : NAN;
        double old_wt = 1.;

        for (int i = 1; i < N; i++)
        {
            std::optional<double> cur = vals[i];
            is_observation = cur.has_value();
            nobs += is_observation;
            if (weighted.has_value())
            {
                if (is_observation || not ignore_na)
                {
                    if (normalize)
                    {
                        if (use_deltas)
                        {
                            old_wt *= std::pow(old_wt_factor, deltas[i - 1]);
                        }
                        else
                        {
                            old_wt *= old_wt_factor;
                        }
                    }
                    else
                    {
                        weighted = old_wt_factor * weighted.value();
                    }
                    if (is_observation)
                    {
                        if (normalize)
                        {
                            // avoid numerical errors on constant series
                            if (weighted != cur)
                            {
                                weighted = old_wt * weighted.value() +
                                    new_wt * cur.value();
                                weighted.value() /= (old_wt + new_wt);
                            }
                            if (adjust)
                            {
                                old_wt += new_wt;
                            }
                            else
                            {
                                old_wt = 1.0;
                            }
                        }
                        else
                        {
                            weighted.value() += cur.value();
                        }
                    }
                }
            }
            else if (is_observation)
            {
                weighted = cur;
            }
            output[i] = (nobs >= minp and weighted.has_value()) ?
                *weighted :
                std::numeric_limits<double>::quiet_NaN();
        }
        return output;
    }

    Series Series::to_datetime() const {
        return cast(TimestampTypePtr);
    }

    Series Series::to_datetime(std::string const& format) const
    {
        return strptime(format, arrow::TimeUnit::NANO, false);
    }

    Series Series::if_else(const Series &truth_values, const Series &other) const {
        return ValidateHelper(arrow::compute::IfElse(truth_values.m_array,
                                                        m_array,
                                                        other.m_array));
    }

    Series Series::if_else(const Series &truth_values, const Scalar &other) const {
        return ValidateHelper(arrow::compute::IfElse(truth_values.m_array,
                                                     m_array,
                                                     other.scalar));
    }

    GenericFunctionSeriesReturn(is_finite)
    GenericFunctionSeriesReturn(is_valid)
    GenericFunctionSeriesReturn(is_null)
    GenericFunctionSeriesReturn(is_infinite)

    bool Series::is_unique() const
    {
        return unique().size() == size();
    }

    Series Series::where(Series const &cond, Series const &other) const
    {
        return if_else(cond, other);
    }

    Series Series::where(Series const &cond, Scalar const &other) const
    {
        return if_else(cond, other);
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

    Series DateTimeLike::week(bool week_starts_monday,bool count_from_zero, bool first_week_is_fully_in_year) const {
        auto opt = arrow::compute::WeekOptions{week_starts_monday, count_from_zero, first_week_is_fully_in_year};
        return ValidateHelper(arrow::compute::CallFunction("week", {m_array}, &opt));
    }

    Series DateTimeLike::day_time_interval_between(const Series &other) const {
        return ValidateHelper(arrow::compute::CallFunction("day_time_interval_between", {m_array, other.m_array}));
    }

    Series DateTimeLike::days_between(const Series &other) const {
        return ValidateHelper(arrow::compute::CallFunction("days_between", {m_array, other.m_array}));
    }

    Series DateTimeLike::hours_between(const Series &other) const {
        return ValidateHelper(arrow::compute::CallFunction("hours_between", {m_array, other.m_array}));
    }

    Series DateTimeLike::microseconds_between(const Series &other) const {
        return ValidateHelper(arrow::compute::CallFunction("microseconds_between", {m_array, other.m_array}));
    }

    Series DateTimeLike::milliseconds_between(const Series &other) const {
        return ValidateHelper(arrow::compute::CallFunction("milliseconds_between", {m_array, other.m_array}));
    }

    Series DateTimeLike::minutes_between(const Series &other) const {
        return ValidateHelper(arrow::compute::CallFunction("minutes_between", {m_array, other.m_array}));
    }

    Series DateTimeLike::month_day_nano_interval_between(const Series &other) const {
        return ValidateHelper(arrow::compute::CallFunction("month_day_nano_interval_between", {m_array, other.m_array}));
    }

    Series DateTimeLike::month_interval_between(const Series &other) const {
        return ValidateHelper(arrow::compute::CallFunction("month_interval_between", {m_array, other.m_array}));
    }

    Series DateTimeLike::nanoseconds_between(const Series &other) const {
        return ValidateHelper(arrow::compute::CallFunction("nanoseconds_between", {m_array, other.m_array}));
    }

    Series DateTimeLike::quarters_between(const Series &other) const {
        return ValidateHelper(arrow::compute::CallFunction("quarters_between", {m_array, other.m_array}));
    }

    Series DateTimeLike::seconds_between(const Series &other) const {
        return ValidateHelper(arrow::compute::CallFunction("seconds_between", {m_array, other.m_array}));
    }

    Series DateTimeLike::weeks_between(const Series &other) const {
        return ValidateHelper(arrow::compute::CallFunction("weeks_between", {m_array, other.m_array}));
    }

    Series DateTimeLike::years_between(const Series &other) const {
        return ValidateHelper(arrow::compute::CallFunction("years_between", {m_array, other.m_array}));
    }
}