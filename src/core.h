#pragma once
//
// Created by dewe on 12/28/22.
//

#include "boost/date_time/gregorian/gregorian.hpp"
#include "boost/date_time/local_time/local_time.hpp"//include all types plus i/o
#include "boost/date_time/posix_time/posix_time.hpp"
#include "chrono"
#include "random.h"
#include "ranges"
#include <arrow/api.h>
#include <arrow/compute/api_scalar.h>
#include <cmath>
#include <rapidjson/document.h>
#include <range/v3/to_container.hpp>


namespace arrow {
    using arrow::internal::checked_pointer_cast;
    using arrow::internal::checked_cast;

}

using namespace boost::posix_time;
using namespace boost::gregorian;

namespace pd {

    const boost::posix_time::ptime EPOCH(boost::gregorian::date(1970, 1, 1));

const auto NULL_INDEX = std::shared_ptr<arrow::Array>{ nullptr };
using ScalarPtr = std::shared_ptr<arrow::Scalar>;
using ArrayPtr = std::shared_ptr<arrow::Array>;

template<class T>
using TableWithType = std::map<std::string, std::vector<T>>;

using ArrayTable = std::map<std::string, ArrayPtr>;
using TablePtr = std::shared_ptr<arrow::Table>;

typedef boost::date_time::subsecond_duration<time_duration, 1000000000> nanosec;
typedef boost::date_time::subsecond_duration<time_duration, 1000000000> nanoseconds;

enum class FillMethod
{
    BFill,
    FFill,
    Nearest
};

enum class CorrelationType
{
    Pearson,
    Kendall,
    Spearman
};

enum struct EWMAlphaType
{
    CenterOfMass,
    Span,
    HalfLife,
    Alpha
};

enum struct EWMAgg
{
    Mean,
    Var,
    StdDev,
};

enum class JoinType
{
    Inner,
    Outer
};

struct TimeGrouperOrigin
{
    enum Type
    {
        Epoch, // origin is 1970-01-01
        Start, // origin is the first value of the timeseries
        StartDay, // origin is the first day at midnight of the timeseries
        End, // origin is the last value of the timeseries
        EndDay, // origin is the ceiling midnight of the last day
        Custom
    } type{StartDay};
    ptime custom{};
};

enum class AxisType
{
    Index,
    Columns,
};

struct Slice
{
    int64_t start = 0;
    int64_t end = 0;

    Slice& normalize(int64_t size)
    {
        if (start < 0)
        {
            start = size + start;
        }
        if (end < 0)
        {
            end = size + end;
        }
        if (start < 0 || start >= size || end < 0 || end > size)
        {
            throw std::runtime_error("Invalid slice indices");
        }
        return *this;
    }
};

struct DateOffset
{
    enum Type
    {
        Day,
        MonthEnd,
        QuarterStart,
        QuarterEnd,
        WeekStart,
        WeekEnd,
        MonthStart,
        YearEnd,
        YearStart
    } type;

    int multiplier{ 1 };
    greg_weekday startDay{Sunday};

    inline friend date operator+(const date& x, const DateOffset& dateOffset)
    {
        return add(x, dateOffset);
    }

    inline DateOffset operator*(int x) const
    {
        return { type, multiplier * x };
    }

    friend inline DateOffset operator*(DateOffset::Type _type, int x)
    {
        return { _type, x };
    }

    inline friend date operator-(const date& x, DateOffset dateOffset)
    {
        dateOffset.multiplier *= -1;
        return add(x, dateOffset);
    }

    static std::optional<DateOffset> FromString(const std::string& code);

private:
    static date add(date, const DateOffset&);
};

struct DateSlice
{
    std::optional<date> start{};
    std::optional<date> end = {};
};

    struct DateTimeSlice
    {
        std::optional<ptime> start{};
        std::optional<ptime> end = {};
    };

struct StringSlice
{
    std::optional<std::string> start{};
    std::optional<std::string> end = {};
};

arrow::compute::CalendarUnit getCalendarUnit(char start_unit);

template<class T>
__always_inline T ReturnOrThrowOnFailure(arrow::Result<T>&& result)
{
    if (result.ok())
        return result.MoveValueUnsafe();
    throw std::runtime_error(result.status().ToString());
}

__always_inline void ThrowOnFailure(arrow::Status&& status)
{
    if (status.ok())
        return;
    throw std::runtime_error(status.ToString());
}

class Series ReturnSeriesOrThrowOnError(arrow::Result<arrow::Datum>&& result);


template<typename T> requires std::is_floating_point_v<T>
std::vector<bool> makeValidFlags(std::vector<T> const &arr) {
    return ranges::to<std::vector>(arr | std::views::transform([](T x){
        return ! std::isnan(x);
    }));
}

struct DateRangeSpec
{
    std::string start{}, end{};
    std::optional<int> periods;
    std::string tz;
    std::string freq{ 'D' };
};

/**
 * Converts unix timestamp (nanoseconds) to ptime.
 */
inline auto toTimeNanoSecPtime(int64_t nanoseconds_since_epoch)
{
    auto div = std::div(nanoseconds_since_epoch, 1000000000L);
    return from_time_t(div.quot) + nanoseconds(div.rem);
}

 inline auto toTimeNanoSecPtime(ScalarPtr const& timeStampNanoSec)
{
    return toTimeNanoSecPtime(std::dynamic_pointer_cast<arrow::TimestampScalar>(timeStampNanoSec)->value);
}

 inline auto toTimeNanoSecPtimeFromInt64(ScalarPtr const& timeStampNanoSec)
{
    return toTimeNanoSecPtime(std::dynamic_pointer_cast<arrow::Int64Scalar>(timeStampNanoSec)->value);
}

 inline auto toDate(int64_t timeStampNanoSec)
{
    return toTimeNanoSecPtime(timeStampNanoSec).date();
}

std::pair<std::string, int> splitTimeSpan(std::string const& freq);

time_duration duration_from_string(std::string const& freq_unit,
                                   int freq_value);

inline time_duration duration_from_string(std::string const& freq,
                                   std::optional<int> freq_value_override=std::nullopt)
{
    auto [freq_unit, freq_value] = splitTimeSpan(freq);
    return duration_from_string(freq_unit, freq_value_override.value_or(freq_value));
}

/**
 * Converts ptime to nanosecond since Unix epoch.
 */
template<class T = int64_t>
T fromPTime(ptime const& date_time) {
    boost::posix_time::time_duration duration = date_time - EPOCH;
    return duration.total_nanoseconds();
}

inline int64_t toTimestampNS(const std::string& date_string)
{
    return fromPTime( boost::posix_time::from_iso_extended_string(date_string));
}

inline std::shared_ptr<arrow::TimestampArray> toDateTime(
    std::vector<int64_t> const& timestamps,
    arrow::TimeUnit::type unit = arrow::TimeUnit::NANO,
    std::string const& tz = "")
{
    arrow::TimestampBuilder builder{ arrow::timestamp(unit, tz), arrow::default_memory_pool() };
    pd::ThrowOnFailure(builder.AppendValues(timestamps));
    const auto array = pd::ReturnOrThrowOnFailure(builder.Finish());
    return dynamic_pointer_cast<arrow::TimestampArray>(array);
}

inline std::shared_ptr<arrow::RecordBatch> concatenateArraysToRecordBatch(
    const std::shared_ptr<arrow::RecordBatch>& originalBatch,
    const std::shared_ptr<arrow::Array>& newArray,
    std::string const& newField)
{
    // Create a new schema with the additional field
    auto fields = originalBatch->schema()->fields();
    fields.push_back(arrow::field(newField, newArray->type()));

    // Create a new RecordBatch with the additional array
    auto columns = originalBatch->columns();
    columns.push_back(newArray);

    return arrow::RecordBatch::Make(arrow::schema(fields), newArray->length(), columns);
}

/**
 * Converts date to nanoseconds since Unix epoch.
 */
template<class T = int64_t>
inline T fromDate(date const& day)
{
    return fromPTime<T>(ptime(day));
}

std::pair<std::string, int> splitTimeSpan(std::string const& freq);

std::shared_ptr<arrow::TimestampArray> date_range(
    date const& start,
    date const& end,
    const DateOffset& freq,
    std::string const& tz = "");

std::shared_ptr<arrow::TimestampArray> date_range(
    date const& start,
    int period,
    const DateOffset& freq,
    std::string const& tz = "");

inline std::shared_ptr<arrow::TimestampArray> date_range(
    date const& start,
    date const& end,
    std::string const& freq = "1D",
    std::string const& tz = "")
{
    return date_range(start, end, *DateOffset::FromString(freq), tz);
}

inline std::shared_ptr<arrow::TimestampArray> date_range(
    date const& start,
    int period,
    std::string const& freq = "1D",
    std::string const& tz = "")
{
    return date_range(start, period, *DateOffset::FromString(freq), tz);
}

std::shared_ptr<arrow::TimestampArray> date_range(
    ptime const& start,
    ptime const& end,
    std::string const& freq = "1T",
    std::string const& tz = "");

std::shared_ptr<arrow::TimestampArray> date_range(
    ptime const& start,
    int period,
    std::string const& freq = "1T",
    std::string const& tz = "");

std::shared_ptr<arrow::TimestampArray> date_range(
    ptime const& start,
    ptime const& end,
    time_duration const& freq,
    std::string const& tz = "");

std::shared_ptr<arrow::TimestampArray> date_range(
    ptime const& start,
    int period,
    time_duration const& freq,
    std::string const& tz = "");

inline std::shared_ptr<arrow::TimestampArray> any_date_range(
    std::variant<date, ptime> const& start,
    int period,
    std::optional<std::string> const& offset={},
    std::string const& tz = "")
{
    if (std::holds_alternative<date>(start))
    {
        return pd::date_range(std::get<date>(start), period, offset.value_or("1D"), tz);
    }
    else
    {
        return pd::date_range(std::get<ptime>(start), period, offset.value_or("1T"), tz);
    }
}

std::shared_ptr<arrow::Int64Array> range(int64_t start, int64_t end);
std::shared_ptr<arrow::UInt64Array> range(::uint64_t start, uint64_t end);

inline std::shared_ptr<arrow::TimestampScalar> fromDateTime(date const& dt)
{
    return std::make_shared<arrow::TimestampScalar>(fromDate(dt), arrow::TimeUnit::NANO);
}

inline std::shared_ptr<arrow::TimestampScalar> fromDateTime(ptime const& dt)
{
    return std::make_shared<arrow::TimestampScalar>(fromPTime(dt), arrow::TimeUnit::NANO);
}

std::shared_ptr<arrow::DataType> promoteTypes(std::vector<std::shared_ptr<arrow::DataType>> const& types);

const std::shared_ptr<arrow::DataType> TimestampTypePtr =
    std::make_shared<arrow::TimestampType>(arrow::TimeUnit::NANO, "");

const std::shared_ptr<arrow::DataType> BooleanTypePtr = std::make_shared<arrow::BooleanType>();

const std::shared_ptr<arrow::DataType> DoubleTypePtr =

    std::make_shared<arrow::DoubleType>();
const std::shared_ptr<arrow::DataType> Int64TypePtr = std::make_shared<arrow::Int64Type>();

class Scalar;
} // namespace pd


namespace arrow {
    template<typename T>
    struct ArrayT {
        static auto Make(std::vector<T> const &x, std::vector<bool> const &map) {
            auto builder = std::make_shared<typename arrow::CTypeTraits<T>::BuilderType>();
            pd::ThrowOnFailure(builder->AppendValues(x, map));
            return std::dynamic_pointer_cast<typename arrow::CTypeTraits<T>::ArrayType>(
                    builder->Finish().MoveValueUnsafe());
        }

        static auto Make(std::vector<T> const &x) {
            auto builder = std::make_shared<typename arrow::CTypeTraits<T>::BuilderType>();
            if constexpr (std::is_floating_point_v<T>)
            {
                return Make(x, pd::makeValidFlags(x));
            }
            pd::ThrowOnFailure(builder->AppendValues(x));
            return std::dynamic_pointer_cast<typename arrow::CTypeTraits<T>::ArrayType>(
                    builder->Finish().MoveValueUnsafe());
        }

        static auto Make(std::valarray<T> const &x) {
            auto builder = std::make_shared<typename arrow::CTypeTraits<T>::BuilderType>();
            if constexpr (std::is_floating_point_v<T>)
            {
                std::vector<bool> flags{x.size(), true};
                std::transform(std::begin(x), std::end(x), flags.begin(), [](T v){
                   return !std::isnan(v);
                });
                pd::ThrowOnFailure(builder->AppendValues(std::begin(x), std::end(x), flags));
            }else {
                pd::ThrowOnFailure(builder->AppendValues(std::begin(x), std::end(x)));
            }
            return std::dynamic_pointer_cast<typename arrow::CTypeTraits<T>::ArrayType>(
                    builder->Finish().MoveValueUnsafe());
        }
    };

    struct ScalarArray {
        static std::shared_ptr<arrow::Array> Make(arrow::ScalarVector const &x,
                                                  const std::shared_ptr<arrow::DataType> &dt = nullptr) {
            if (x.empty()) {
                if (dt) {
                    return pd::ReturnOrThrowOnFailure(arrow::MakeEmptyArray(dt));
                } else {
                    throw std::runtime_error("Empty ScalarArray requires a valid data_Type is passed");
                }
            }
            auto builder = arrow::MakeBuilder(x.back()->type).MoveValueUnsafe();
            pd::ThrowOnFailure(builder->AppendScalars(x));
            return builder->Finish().MoveValueUnsafe();
        }

        static std::shared_ptr<arrow::Array> Make(std::vector<pd::Scalar> const &x);
    };

    struct DateArray : ArrayT<date> {
        static auto Make(std::vector<date> const &dates) {
            std::vector<int64_t> timestamps64(dates.size());
            std::ranges::transform(dates, timestamps64.begin(), [](auto const &t) { return pd::fromDate(t); });
            return pd::toDateTime(timestamps64);
        }

        static auto Make(std::vector<date> const &x, std::vector<bool> const &map) {
            TimestampBuilder builder(std::make_shared<arrow::TimestampType>(arrow::TimeUnit::NANO), default_memory_pool());

            std::vector<int64_t> ts(x.size());
            std::ranges::transform(x, ts.begin(), [](auto const &t) { return pd::fromDate(t); });

            pd::ThrowOnFailure(builder.AppendValues(ts, map));

            return std::dynamic_pointer_cast<arrow::TimestampArray>(builder.Finish().MoveValueUnsafe());
        }
    };

    struct DateTimeArray : ArrayT<ptime> {
        static std::shared_ptr<arrow::TimestampArray> Make(std::vector<ptime> const &x) {
            std::vector<int64_t> timestamps64(x.size());
            std::ranges::transform(x, timestamps64.begin(), [](auto const &t) { return pd::fromPTime(t); });
            return pd::toDateTime(timestamps64);
        }

        static std::shared_ptr<arrow::TimestampArray> Make(std::vector<ptime> const &x, std::vector<bool> const &map) {
            TimestampBuilder builder(std::make_shared<arrow::TimestampType>(arrow::TimeUnit::NANO), default_memory_pool());

            std::vector<int64_t> ts(x.size());
            std::ranges::transform(x, ts.begin(), [](auto const &t) { return pd::fromPTime(t); });

            pd::ThrowOnFailure(builder.AppendValues(ts, map));

            return std::dynamic_pointer_cast<arrow::TimestampArray>(builder.Finish().MoveValueUnsafe());
        }
    };

    template<class T>
    std::shared_ptr<arrow::Array> ArrayFromJSON(std::string_view json) {
        rapidjson::Document doc;
        doc.Parse(json.data());
        if (!doc.IsArray()) {
            throw std::runtime_error("Input JSON is not an array");
        }

        typename arrow::CTypeTraits<T>::BuilderType builder;

        for (const auto &val: doc.GetArray()) {
            if (val.IsNull()) {
                pd::ThrowOnFailure(builder.AppendNull());
            } else if (val.IsInt()) {
                if constexpr (std::is_integral_v<T>)
                    pd::ThrowOnFailure(builder.Append(val.GetInt()));
                else
                    throw std::runtime_error("Unsupported data type");
            } else if (val.IsBool()) {
                pd::ThrowOnFailure(builder.Append(val.GetBool()));
            } else if (val.IsInt64()) {
                if constexpr (std::is_integral_v<T>)
                    pd::ThrowOnFailure(builder.Append(val.GetInt64()));
                else
                    throw std::runtime_error("Unsupported data type");
            } else if (val.IsUint64()) {
                if constexpr (std::is_integral_v<T>)
                    pd::ThrowOnFailure(builder.Append(val.GetUint64()));
                else
                    throw std::runtime_error("Unsupported data type");
            } else if (val.IsUint()) {
                if constexpr (std::is_integral_v<T>)
                    pd::ThrowOnFailure(builder.Append(val.GetUint()));
                else
                    throw std::runtime_error("Unsupported data type");
            } else if (val.IsFloat()) {
                if constexpr (std::is_floating_point_v<T>)
                    pd::ThrowOnFailure(builder.Append(val.GetFloat()));
                else
                    throw std::runtime_error("Unsupported data type");
            } else if (val.IsDouble()) {
                if constexpr (std::is_floating_point_v<T>)
                    pd::ThrowOnFailure(builder.Append(val.GetDouble()));
                else
                    throw std::runtime_error("Unsupported data type");
            } else if (val.IsString()) {
                if constexpr (std::is_same<T, std::string>::value)
                    pd::ThrowOnFailure(builder.Append(val.GetString()));
                else
                    throw std::runtime_error("Unsupported data type");
            } else {
                throw std::runtime_error("Unsupported data type in JSON array");
            }
        }

        return pd::ReturnOrThrowOnFailure(builder.Finish());
    }

} // namespace arrow