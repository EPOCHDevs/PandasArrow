#pragma once
//
// Created by dewe on 12/28/22.
//

#include <arrow/api.h>
#include <arrow/testing/gtest_util.h>
#include <cmath>
#include <rapidjson/document.h>
#include "boost/date_time/gregorian/gregorian.hpp"
#include "boost/date_time/local_time/local_time.hpp" //include all types plus i/o
#include "boost/date_time/posix_time/posix_time.hpp"
#include "chrono"
#include "random.h"
#include "ranges"


using namespace boost::posix_time;
using namespace boost::gregorian;

namespace pd {

const auto NULL_INDEX = std::shared_ptr<arrow::Array>{nullptr};
using ScalarPtr = std::shared_ptr<arrow::Scalar>;
using ArrayPtr = std::shared_ptr<arrow::Array>;

template<class T>
using TableWithType = std::map<std::string, std::vector<T>>;

using ArrayTable = std::map<std::string, ArrayPtr>;
using TablePtr = std::shared_ptr<arrow::Table>;

typedef boost::date_time::subsecond_duration<time_duration,1000000000> nanosec;
typedef boost::date_time::subsecond_duration<time_duration,1000000000> nanoseconds;

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

enum class JoinType
{
    Inner,
    Outer
};

enum class TimeGrouperOrigin
{
    Epoch, // origin is 1970-01-01
    Start, // origin is the first value of the timeseries
    StartDay, // origin is the first day at midnight of the timeseries
    End, // origin is the last value of the timeseries
    EndDay, // origin is the ceiling midnight of the last day
};

enum class AxisType
{
    Index,
    Columns
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

struct DateTimeSlice
{
    std::optional<date> start{};
    std::optional<date> end = {};
};

struct StringSlice
{
    std::optional<std::string> start{};
    std::optional<std::string> end = {};
};

template<class T>
__always_inline T ReturnOrThrowOnFailure(arrow::Result<T>&& result)
{
    if (result.ok()) return result.MoveValueUnsafe();
    throw std::runtime_error(result.status().ToString());
}

__always_inline void ThrowOnFailure(arrow::Status&& status)
{
    if (status.ok()) return;
    throw std::runtime_error(status.ToString());
}

class Series ReturnSeriesOrThrowOnError(arrow::Result<arrow::Datum>&& result);

template<class RetT>
inline auto ReturnScalarOrThrowOnError(auto&& result)
{
    if (result.ok())
    {
        auto res = result->template scalar_as<
            typename arrow::CTypeTraits<RetT>::ScalarType>();
        return res.is_valid ? res.value :
                              std::numeric_limits<RetT>::quiet_NaN();
    }
    throw std::runtime_error(result.status().ToString());
}

template<class T>
inline bool isValid(T const& x)
{
    if constexpr (std::is_floating_point_v<T>)
    {
        return not std::isnan(x);
    }
    else if constexpr (std::is_same_v<T, std::string>)
    {
        return not x.empty();
    }
    return true;
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
static inline auto toTimeNanoSecPtime(uint64_t timeStampNanoSec)
{
    return from_time_t(std::chrono::duration_cast<std::chrono::seconds>(
                           std::chrono::nanoseconds(timeStampNanoSec))
                           .count());
}

static inline auto toTimeNanoSecPtime(ScalarPtr const& timeStampNanoSec)
{
    return toTimeNanoSecPtime(
        std::dynamic_pointer_cast<arrow::TimestampScalar>(timeStampNanoSec)
            ->value);
}

static inline auto toDate(int64_t timeStampNanoSec)
{
    return toTimeNanoSecPtime(timeStampNanoSec).date();
}

inline int64_t toTimestampNS(const std::string& date_string)
{
    boost::posix_time::ptime date_time =
        boost::posix_time::from_iso_extended_string(date_string);
    boost::posix_time::ptime epoch(boost::gregorian::date(1970, 1, 1));
    boost::posix_time::time_duration duration = date_time - epoch;
    return duration.total_nanoseconds();
}

/**
 * Converts ptime to nanosecond since Unix epoch.
 */
template<class T = int64_t>
static inline T fromPTime(ptime const& _time)
{
    return static_cast<T>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                              std::chrono::seconds(to_time_t(_time)))
                              .count());
}

inline std::shared_ptr<arrow::TimestampArray> toDateTime(
    std::vector<int64_t> const& timestamps,
    arrow::TimeUnit::type unit = arrow::TimeUnit::NANO,
    std::string const& tz = "")
{
    arrow::TimestampBuilder builder{ arrow::timestamp(unit, tz),
                                     arrow::default_memory_pool() };
    ABORT_NOT_OK(builder.AppendValues(timestamps));
    ASSIGN_OR_ABORT(auto array, builder.Finish());
    return dynamic_pointer_cast<arrow::TimestampArray>(array);
}

/**
 * Converts date to nanoseconds since Unix epoch.
 */
template<class T = int64_t>
static inline T fromDate(date const& day)
{
    return fromPTime<T>(ptime(day));
}

inline date firstDateOfYear(date x)
{
    while ((x - days(1)).year() == x.year())
    {
        x -= days(1);
    }
    return x;
}

inline date lastDateOfYear(date x)
{
    while ((x + days(1)).year() == x.year())
    {
        x += days(1);
    }
    return x;
}

inline date firstDateOfMonth(date x)
{
    while ((x - days(1)).month() == x.month())
    {
        x -= days(1);
    }
    return x;
}

std::pair<std::string, int> splitTimeSpan(std::string const& freq);

std::shared_ptr<arrow::TimestampArray> date_range(
    date const& start,
    date const& end,
    std::string const& freq = "D",
    std::string const& tz = "");

std::shared_ptr<arrow::TimestampArray> date_range(
    date const& start,
    int period,
    std::string const& freq = "D",
    std::string const& tz = "");

std::shared_ptr<arrow::TimestampArray> date_range(
    ptime const& start,
    ptime const& end,
    std::string const& freq = "T",
    std::string const& tz = "");

std::shared_ptr<arrow::TimestampArray> date_range(
    ptime const& start,
    int period,
    std::string const& freq = "T",
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

std::shared_ptr<arrow::Int64Array> range(int64_t start, int64_t end);
std::shared_ptr<arrow::UInt64Array> range(::uint64_t start, uint64_t end);

inline std::shared_ptr<arrow::TimestampScalar> fromDateTime(date const& dt)
{
    return std::make_shared<arrow::TimestampScalar>(
        fromDate(dt),
        arrow::TimeUnit::NANO);
}

inline std::shared_ptr<arrow::TimestampScalar> fromDateTime(ptime const& dt)
{
    return std::make_shared<arrow::TimestampScalar>(
        fromPTime(dt),
        arrow::TimeUnit::NANO);
}

std::shared_ptr<arrow::DataType> promoteTypes(
    std::vector<std::shared_ptr<arrow::DataType>> const& types);

const std::shared_ptr<arrow::DataType> TimestampTypePtr =
    std::make_shared<arrow::TimestampType>(arrow::TimeUnit::NANO, "");

const std::shared_ptr<arrow::DataType> BooleanTypePtr =
    std::make_shared<arrow::BooleanType>();

const std::shared_ptr<arrow::DataType> DoubleTypePtr =

    std::make_shared<arrow::DoubleType>();
const std::shared_ptr<arrow::DataType> Int64TypePtr =
    std::make_shared<arrow::Int64Type>();

class Scalar;
}


namespace arrow {
template<typename T>
struct ArrayT
{
        static auto Make(std::vector<T> const& x)
        {
        auto builder =
            std::make_shared<typename arrow::CTypeTraits<T>::BuilderType>();
        pd::ThrowOnFailure(builder->AppendValues(x));
        return std::dynamic_pointer_cast<
            typename arrow::CTypeTraits<T>::ArrayType>(
            builder->Finish().MoveValueUnsafe());
        }

        static auto Make(std::vector<T> const& x, std::vector<bool> const& map)
        {
            auto builder =
                std::make_shared<typename arrow::CTypeTraits<T>::BuilderType>();
            pd::ThrowOnFailure(builder->AppendValues(x, map));
            return std::dynamic_pointer_cast<
                typename arrow::CTypeTraits<T>::ArrayType>(
                builder->Finish().MoveValueUnsafe());
        }
};

struct ScalarArray
{
        static std::shared_ptr<arrow::Array> Make(arrow::ScalarVector const& x)
        {
            if (x.empty())
            {
            return { nullptr };
            }
            auto builder = arrow::MakeBuilder(x.back()->type).MoveValueUnsafe();
            ABORT_NOT_OK(builder->AppendScalars(x));
            return builder->Finish().MoveValueUnsafe();
        }

        static std::shared_ptr<arrow::Array> Make(std::vector<pd::Scalar> const& x);

};

struct DateArray : ArrayT<date>
{
        static auto Make(std::vector<date> const& dates)
        {
            std::vector<int64_t> timestamps64(dates.size());
            std::ranges::transform(
                dates,
                timestamps64.begin(),
                [](auto const& t) { return pd::fromDate(t); });
            return pd::toDateTime(timestamps64);
        }

        static auto Make(
            std::vector<date> const& x,
            std::vector<bool> const& map)
        {
            TimestampBuilder builder(
                std::make_shared<arrow::TimestampType>(arrow::TimeUnit::NANO),
                default_memory_pool());

            std::vector<int64_t> ts(x.size());
            std::ranges::transform(
                x,
                ts.begin(),
                [](auto const& t) { return pd::fromDate(t); });

            ABORT_NOT_OK(builder.AppendValues(ts, map));

            return std::dynamic_pointer_cast<arrow::TimestampArray>(
                builder.Finish().MoveValueUnsafe());
        }
};

struct DateTimeArray : ArrayT<ptime>
{
        static std::shared_ptr<arrow::TimestampArray> Make(std::vector<ptime> const& x)
        {
            std::vector<int64_t> timestamps64(x.size());
            std::ranges::transform(
                x,
                timestamps64.begin(),
                [](auto const& t) { return pd::fromPTime(t); });
            return pd::toDateTime(timestamps64);
        }

        static std::shared_ptr<arrow::TimestampArray> Make(
            std::vector<ptime> const& x,
            std::vector<bool> const& map)
        {
            TimestampBuilder builder(
                std::make_shared<arrow::TimestampType>(arrow::TimeUnit::NANO),
                default_memory_pool());

            std::vector<int64_t> ts(x.size());
            std::ranges::transform(
                x,
                ts.begin(),
                [](auto const& t) { return pd::fromPTime(t); });

            pd::ThrowOnFailure(builder.AppendValues(ts, map));

            return std::dynamic_pointer_cast<arrow::TimestampArray>(
                builder.Finish().MoveValueUnsafe());
        }
};

template<class T>
std::shared_ptr<arrow::Array> ArrayFromJSON(std::string_view json)
{
        rapidjson::Document doc;
        doc.Parse(json.data());
        if (!doc.IsArray())
        {
            throw std::runtime_error("Input JSON is not an array");
        }

        typename arrow::CTypeTraits<T>::BuilderType builder;

        for (const auto& val : doc.GetArray())
        {
            if (val.IsNull())
            {
            pd::ThrowOnFailure(builder.AppendNull());
            }
            else if (val.IsInt())
            {
            if constexpr (std::is_integral_v<T>)
                pd::ThrowOnFailure(builder.Append(val.GetInt()));
            else
                throw std::runtime_error("Unsupported data type");
            }
            else if (val.IsBool())
            {
                pd::ThrowOnFailure(builder.Append(val.GetBool()));
            }
            else if (val.IsInt64())
            {
            if constexpr (std::is_integral_v<T>)
                pd::ThrowOnFailure(builder.Append(val.GetInt64()));
            else
                throw std::runtime_error("Unsupported data type");
            }
            else if (val.IsUint64())
            {
            if constexpr (std::is_integral_v<T>)
                pd::ThrowOnFailure(builder.Append(val.GetUint64()));
            else
                throw std::runtime_error("Unsupported data type");
            }
            else if (val.IsUint())
            {
            if constexpr (std::is_integral_v<T>)
                pd::ThrowOnFailure(builder.Append(val.GetUint()));
            else
                throw std::runtime_error("Unsupported data type");
            }
            else if (val.IsFloat())
            {
            if constexpr (std::is_floating_point_v<T>)
                pd::ThrowOnFailure(builder.Append(val.GetFloat()));
            else
                throw std::runtime_error("Unsupported data type");
            }
            else if (val.IsDouble())
            {
            if constexpr (std::is_floating_point_v<T>)
                pd::ThrowOnFailure(builder.Append(val.GetDouble()));
            else
                throw std::runtime_error("Unsupported data type");
            }
            else if (val.IsString())
            {
            if constexpr (std::is_same<T, std::string>::value)
                pd::ThrowOnFailure(builder.Append(val.GetString()));
            else
                throw std::runtime_error("Unsupported data type");
            }
            else
            {
            throw std::runtime_error("Unsupported data type in JSON array");
            }
        }

        return pd::ReturnOrThrowOnFailure(builder.Finish());
}

}