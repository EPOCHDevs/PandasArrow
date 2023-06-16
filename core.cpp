//
// Created by dewe on 12/31/22.
//
#include "core.h"
#include <arrow/compute/cast.h>
#include <boost/chrono/duration.hpp>
#include <future>
#include "arrow/compute/exec.h"
#include "dataframe.h"
#include "group_by.h"

namespace pd {

date DateOffset::add(const date& x, Type type, int n)
{
    using namespace boost::gregorian;

    date result = x;

    switch (type)
    {
        case MonthEnd:
            result += months(n);
            result = result.end_of_month();
            break;

        case QuarterStart:
            result += months(3 * n);
            result = date(result.year(), (result.month() - 1) / 3 * 3 + 1, 1);
            break;

        case QuarterEnd:
            result += months(3 * n);
            result = date(result.year(), (result.month() - 1) / 3 * 3 + 3, 1) - days(1);
            break;

        case Weekly:
            result += weeks(n);
            break;

        case MonthStart:
            result += months(n);
            result = date(result.year(), result.month(), 1);
            break;

        case YearEnd:
            result += years(n);
            result = date(result.year(), Dec, 31);
            break;

        case YearStart:
            result += years(n);
            result = date(result.year(), Jan, 1);
            break;

        default:
            result += days(n);
    }

    return result;
}
std::optional<DateOffset> DateOffset::FromString(const string& code)
{
    const auto [freq_unit, mul] = splitTimeSpan(code);
    DateOffset::Type type{ DateOffset::Day };

    if (freq_unit == "D")
    {
        type = DateOffset::Day;
    }
    else if (freq_unit == "W")
    {
        type = DateOffset::Weekly;
    }
    else if (freq_unit == "MS")
    {
        type = DateOffset::MonthStart;
    }
    else if (freq_unit == "M")
    {
        type = DateOffset::MonthEnd;
    }
    else if (freq_unit == "Y")
    {
        type = DateOffset::YearEnd;
    }
    else if (freq_unit == "YS")
    {
        type = DateOffset::YearStart;
    }
    else if (freq_unit == "Q")
    {
        type = DateOffset::QuarterEnd;
    }
    else if (freq_unit == "QS")
    {
        type = DateOffset::QuarterStart;
    }
    else
    {
        return std::nullopt;
    }
    return std::make_optional<DateOffset>(type, mul);
}

std::pair<std::string, int> splitTimeSpan(std::string const& freq)
{
    auto it = std::find_if(freq.begin(), freq.end(), [](int x) { return std::isalpha(x); });

    std::ostringstream ss;
    std::string freq_unit, freqValueStr;
    int freq_value = 1;

    copy(it, freq.end(), std::ostream_iterator<uint8_t>(ss));
    freq_unit = ss.str();

    if (freq.begin() != it)
    {
        std::ostringstream ss2;
        copy(freq.begin(), it, std::ostream_iterator<uint8_t>(ss2));
        freqValueStr = ss2.str();
        freq_value = std::stoi(freqValueStr);
    }
    return { freq_unit, freq_value };
}

template<class Iterator = day_iterator, typename FreqTime = int>
std::shared_ptr<arrow::TimestampArray> date_range(
    date const& start,
    date const& end,
    FreqTime freq = 1,
    std::string const& tz = "")
{
    if (start >= end)
    {
        throw std::runtime_error("start date has to be less than end date");
    }

    if (freq < 1)
    {
        throw std::runtime_error("FREQ must be >= 1");
    }

    auto N = size_t(std::round(double(date_period(start, end).length().days()) / double(freq)));
    std::vector<int64_t> timestamps;
    timestamps.reserve(N);

    for (auto it = Iterator(start, freq); it <= end; ++it)
    {
        timestamps.push_back(fromDate(*it));
    }

    return toDateTime(timestamps, arrow::TimeUnit::NANO, tz);
}

template<class Iterator = day_iterator, typename FreqTime = int>
std::shared_ptr<arrow::TimestampArray> date_range(
    date const& start,
    int period,
    FreqTime freq,
    std::string const& tz = "")
{
    if (period <= 0)
    {
        throw std::runtime_error("period has to be positive");
    }
    if (freq < 1)
    {
        throw std::runtime_error("FREQ must be >= 1");
    }

    Iterator it(start, freq);

    std::vector<int64_t> timestamps(period);

    std::generate_n(
        timestamps.begin(),
        period,
        [&it]()
        {
            auto result = fromDate(*it);
            ++it;
            return result;
        });
    return toDateTime(timestamps);
}

std::shared_ptr<arrow::TimestampArray> switchFunction(
    date const& start,
    auto const& end_or_period,
    const DateOffset& freq,
    std::string const& tz)
{
    switch (freq.type)
    {
        case DateOffset::Day:
            return date_range<day_iterator>(start, end_or_period, freq.multiplier, tz);
        case DateOffset::MonthEnd:
        case DateOffset::MonthStart:
            return date_range<month_iterator>(start, end_or_period, freq.multiplier, tz);
        case DateOffset::QuarterStart:
        case DateOffset::QuarterEnd:
        {
            if (start.month() / 3 != 0)
            {
                throw std::runtime_error("A quarter freq requires month is on a quarter, +/- with DateOffset");
            }
            return date_range<month_iterator>(start, end_or_period, freq.multiplier * 3, tz);
        }
        case DateOffset::Weekly:
            return date_range<week_iterator>(start, end_or_period, freq.multiplier, tz);
        case DateOffset::YearEnd:
        case DateOffset::YearStart:
            return date_range<year_iterator>(start, end_or_period, freq.multiplier, tz);
    }
    return {nullptr};
}

std::shared_ptr<arrow::TimestampArray> switchFunction(
    ptime const& start,
    auto const& end_or_period,
    std::string const& freq,
    std::string const& tz)
{
    return date_range(start, end_or_period, duration_from_string(freq), tz);
}

time_duration duration_from_string(std::string const& freq_unit,
                                   int freq_value)
{
    if (freq_unit == "H" or freq_unit == "hrs")
    {
        return hours(freq_value);
    }
    else if (freq_unit == "T" or freq_unit == "min")
    {
        return minutes(freq_value);
    }
    else if (freq_unit == "S")
    {
        return seconds(freq_value);
    }
    else if (freq_unit == "L" or freq_unit == "ms")
    {
        return milliseconds(freq_value);
    }
    else if (freq_unit == "U" or freq_unit == "us")
    {
        return microseconds(freq_value);
    }
    else if (freq_unit == "N" or freq_unit == "ns")
    {
        return pd::nanoseconds(freq_value);
    }
    throw std::runtime_error(
        "date_range with start:ptime_type is only compatible with "
        "[T/min S L/ms U/us N/ns] freq_unit");
}

std::shared_ptr<arrow::TimestampArray> date_range(
    ptime const& start,
    ptime const& end,
    time_duration const& freq,
    std::string const& tz)
{
    if (start >= end)
    {
        throw std::runtime_error("start date has to be less than end date");
    }

    if (freq.is_negative() or freq.is_zero())
    {
        throw std::runtime_error("FREQ must be positive");
    }

    std::vector<int64_t> timestamps;
    for (auto it = time_iterator(start, freq); it <= end; ++it)
    {
        timestamps.push_back(fromPTime(*it));
    }

    return toDateTime(timestamps, arrow::TimeUnit::NANO, tz);
}

std::shared_ptr<arrow::TimestampArray> date_range(
    ptime const& start,
    int period,
    time_duration const& freq,
    std::string const& tz)
{
    if (period <= 0)
    {
        throw std::runtime_error("period has to be positive");
    }

    if (freq.is_negative() or freq.is_zero())
    {
        throw std::runtime_error("FREQ must be positive");
    }

    time_iterator it(start, freq);
    std::vector<int64_t> timestamps(period);

    std::generate_n(
        timestamps.begin(),
        period,
        [it]() mutable
        {
            auto result = fromPTime(*it);
            ++it;
            return result;
        });

    return toDateTime(timestamps, arrow::TimeUnit::NANO, tz);
}

std::shared_ptr<arrow::TimestampArray> date_range(
    date const& start,
    date const& end,
    const DateOffset& freq,
    std::string const& tz)
{
    return switchFunction(start, end, freq, tz);
}

std::shared_ptr<arrow::TimestampArray> date_range(
    date const& start,
    int period,
    const DateOffset& freq,
    std::string const& tz)
{
    return switchFunction(start, period, freq, tz);
}

std::shared_ptr<arrow::TimestampArray> date_range(
    ptime const& start,
    ptime const& end,
    std::string const& freq,
    std::string const& tz)
{
    return switchFunction(start, end, freq, tz);
}

std::shared_ptr<arrow::TimestampArray> date_range(
    ptime const& start,
    int period,
    std::string const& freq,
    std::string const& tz)
{
    return switchFunction(start, period, freq, tz);
}

std::shared_ptr<arrow::Int64Array> range(int64_t start, int64_t end)
{
    arrow::Int64Builder builder;
    auto rangeView = std::views::iota(start, end);
    ABORT_NOT_OK(builder.AppendValues(rangeView.begin(), rangeView.end()));

    return dynamic_pointer_cast<arrow::Int64Array>(builder.Finish().MoveValueUnsafe());
}

std::shared_ptr<arrow::UInt64Array> range(::uint64_t start, uint64_t end)
{
    arrow::UInt64Builder builder;
    auto rangeView = std::views::iota(start, end);
    ABORT_NOT_OK(builder.AppendValues(rangeView.begin(), rangeView.end()));

    return dynamic_pointer_cast<arrow::UInt64Array>(builder.Finish().MoveValueUnsafe());
}

std::shared_ptr<arrow::Array> combineIndexes(std::vector<Series::ArrayType> const& indexes, bool ignore_index)
{
    if (ignore_index)
    {
        std::vector<std::uint64_t> idx_len(indexes.size());
        std::ranges::transform(indexes, idx_len.begin(), [](Series::ArrayType const& idx) { return idx->length(); });
        return range(0UL, std::accumulate(idx_len.begin(), idx_len.end(), 0UL));
    }

    auto result = arrow::Concatenate(indexes);
    if (result.ok())
        return result.MoveValueUnsafe();

    throw std::runtime_error(result.status().ToString());
}

Series ReturnSeriesOrThrowOnError(arrow::Result<arrow::Datum>&& result)
{
    if (result.ok())
    {
        return pd::Series{ result->make_array(), false, "" };
    }
    throw std::runtime_error(result.status().ToString());
}

std::shared_ptr<arrow::DataType> promoteTypes(const std::vector<std::shared_ptr<arrow::DataType>>& types)
{

    if (types.empty())
    {
        return arrow::null();
    }

    std::shared_ptr<arrow::DataType> common_type = types[0];

    for (size_t i = 1; i < types.size(); i++)
    {

        auto current_type = types[i];
        if (arrow::is_temporal(current_type->id()))
        {
            current_type = arrow::int64();
        }

        if (arrow::is_numeric(current_type->id()))
        {
            if (current_type->id() > common_type->id())
            {
                common_type = types[i];
            }
        }
        else
        {
            return arrow::utf8();
        }
    }

    return common_type;
}
} // namespace pd
std::shared_ptr<arrow::Array> arrow::ScalarArray::Make(const std::vector<pd::Scalar>& x)
{
    if (x.empty())
    {
        return { nullptr };
    }
    auto builder = arrow::MakeBuilder(x.back().value()->type).MoveValueUnsafe();

    ABORT_NOT_OK(builder->Reserve(x.size()));

    for (auto const& sc : x)
    {
        ABORT_NOT_OK(builder->AppendScalar(*sc.value()));
    }

    return builder->Finish().MoveValueUnsafe();
}
