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

using ScalarPtr = std::shared_ptr<arrow::Scalar>;
using ArrayPtr = std::shared_ptr<arrow::Array>;

typedef boost::date_time::subsecond_duration<time_duration,1000000000> nanosec;
typedef boost::date_time::subsecond_duration<time_duration,1000000000> nanoseconds;

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

class Series ValidateHelper(auto&& result);

template<class RetT>
inline auto ValidateHelperScalar(auto&& result)
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
        boost::posix_time::from_iso_string(date_string);
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
static inline int64_t fromDate(date const& day)
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

class DataFrame concat(
    std::vector<pd::DataFrame> const& objs,
    AxisType axis,
    JoinType join,
    bool ignore_index,
    bool sort);

pd::DataFrame concatRows(
    std::vector<pd::DataFrame> const& objs,
    JoinType join = pd::JoinType::Outer,
    bool ignore_index = false,
    bool sort = false);

pd::DataFrame concatColumns(
    std::vector<pd::DataFrame> const& objs,
    JoinType join = pd::JoinType::Outer,
    bool ignore_index = false,
    bool sort = false);

pd::DataFrame concatColumnsUnsafe(std::vector<pd::DataFrame> const& objs);

std::vector<std::shared_ptr<arrow::Field>> mergeColumns(
    std::vector<pd::DataFrame> const& objs,
    JoinType join);

std::shared_ptr<arrow::Array> mergeRows(
    std::vector<pd::DataFrame> const& objs,
    JoinType join);

std::shared_ptr<arrow::Array> combineIndexes(
    std::vector<std::shared_ptr<arrow::Array>> const& indexes,
    bool ignore_index);

const std::shared_ptr<arrow::DataType> TimestampTypePtr =
    std::make_shared<arrow::TimestampType>(arrow::TimeUnit::NANO, "");

const std::shared_ptr<arrow::DataType> BooleanTypePtr =
    std::make_shared<arrow::BooleanType>();

const std::shared_ptr<arrow::DataType> DoubleTypePtr =

    std::make_shared<arrow::DoubleType>();
const std::shared_ptr<arrow::DataType> Int64TypePtr =
    std::make_shared<arrow::Int64Type>();
}


namespace arrow {
template<typename T>
struct ArrayT
{
        static auto Make(std::vector<T> const& x)
        {
        auto builder =
            std::make_shared<typename arrow::CTypeTraits<T>::BuilderType>();
        builder->AppendValues(x);
        return std::dynamic_pointer_cast<
            typename arrow::CTypeTraits<T>::ArrayType>(
            builder->Finish().MoveValueUnsafe());
        }

        static auto Make(std::vector<T> const& x, std::vector<bool> const& map)
        {
            auto builder =
                std::make_shared<typename arrow::CTypeTraits<T>::BuilderType>();
            builder->AppendValues(x, map);
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

            builder.AppendValues(ts, map);

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
                builder.AppendNull();
            }
            else if (val.IsInt())
            {
                builder.Append(val.GetInt());
            }
            else if (val.IsInt64())
            {
                builder.Append(val.GetInt64());
            }
            else if (val.IsDouble())
            {
                builder.Append(val.GetDouble());
            }
            //        else if (val.IsString())
            //        {
            //            builder.Append(val.GetString());
            //        }
            else
            {
                throw std::runtime_error("Unsupported data type in JSON array");
            }
        }
        std::shared_ptr<arrow::Array> array;
        builder.Finish(&array);
        return array;
}

}