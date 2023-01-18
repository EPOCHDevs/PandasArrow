#pragma once
//
// Created by dewe on 12/28/22.
//

#include <arrow/api.h>
#include <cmath>
#include "random.h"
#include "boost/date_time/gregorian/gregorian.hpp"
#include "boost/date_time/local_time/local_time.hpp" //include all types plus i/o
#include "boost/date_time/posix_time/posix_time.hpp"
#include "chrono"
#include "ranges"


using namespace boost::posix_time;
using namespace boost::gregorian;

namespace pd{

    enum class CorrelationType {
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

    struct Slice{
        int64_t start=0;
        int64_t end=0;

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

    struct DateTimeSlice{
        std::optional<date> start{};
        std::optional<date>  end={};
    };

    struct StringSlice{
        std::optional<std::string> start{};
        std::optional<std::string> end={};
    };

    class Series ValidateHelper(auto && result);

    template<class RetT>
    inline auto ValidateHelperScalar(auto && result) {
        if (result.ok()) {
            return result->template scalar_as<typename arrow::CTypeTraits<RetT>::ScalarType>().value;
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
        else if constexpr(std::is_same_v<T, std::string>)
        {
            return not x.empty();
        }
        return true;
    }

    struct DateRangeSpec{
        std::string start{}, end{};
        std::optional<int> periods;
        std::string tz;
        std::string freq{'D'};
    };

    /**
 * Converts unix timestamp (nanoseconds) to ptime.
 */
    static inline auto toTimeNanoSecPtime(uint64_t timeStampNanoSec)
    {
        return from_time_t(
            std::chrono::duration_cast<std::chrono::seconds>(
                std::chrono::nanoseconds(timeStampNanoSec)).count());
    }

    inline int64_t toTimestampNS(const std::string& date_string) {
        boost::posix_time::ptime date_time = boost::posix_time::from_iso_string(date_string);
        boost::posix_time::ptime epoch(boost::gregorian::date(1970,1,1));
        boost::posix_time::time_duration duration = date_time - epoch;
        return duration.total_nanoseconds();
    }

/**
 * Converts ptime to nanosecond since Unix epoch.
 */
    template<class T = int64_t>
    static inline auto fromPTime(ptime const& _time)
    {
        return static_cast<T>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::seconds(to_time_t(_time)))
                .count());
    }

/**
 * Converts date to nanoseconds since Unix epoch.
 */
    template<class T = int64_t>
    static inline auto fromDate(date const& day)
    {
        return fromPTime<T>(ptime(day));
    }

    inline date firstDateOfYear(date x)
    {
        while ( (x - days(1)).year() == x.year())
        {
            x -= days(1);
        }
        return x;
    }

    inline date lastDateOfYear(date x)
    {
        while ( (x + days(1)).year() == x.year())
        {
            x += days(1);
        }
        return x;
    }

    inline date firstDateOfMonth(date x)
    {
        while ( (x - days(1)).month() == x.month())
        {
            x -= days(1);
        }
        return x;
    }

    std::pair<std::string, int> splitTimeSpan(std::string const& freq);

    std::shared_ptr<arrow::Array> date_range(date const& start, date const& end,
                                             std::string const& freq="D",
                                             std::string const& tz="");

    std::shared_ptr<arrow::Array> date_range(date const& start, int period,
                                             std::string const& freq="D",
                                             std::string const& tz="");

    std::shared_ptr<arrow::Array> date_range(ptime const& start, ptime const& end,
                                             std::string const&  freq="T",
                                             std::string const&  tz="");

    std::shared_ptr<arrow::Array> date_range(ptime const& start, int period,
                                             std::string const&  freq="T",
                                             std::string const&  tz="");

    std::shared_ptr<arrow::Array> date_range(
            ptime const& start,
            ptime const& end,
            time_duration const& freq,
            std::string const& tz="");

    std::shared_ptr<arrow::Array> date_range(
            ptime const& start,
            int period,
            time_duration const& freq,
            std::string const& tz="");

    class GroupBy resample(
        class DataFrame const& ax,
        time_duration const& freq,
        bool closed_right=false,
        bool label_right=false,
        std::string const& tz="");

    std::shared_ptr<arrow::Array> range(int64_t start, int64_t end);

    inline std::shared_ptr<arrow::Scalar> fromDateTime(date const& dt)
    {
        return std::make_shared<arrow::TimestampScalar>(fromDate(dt),
                                                        arrow::TimeUnit::NANO);
    }

    inline std::shared_ptr<arrow::Scalar> fromDateTime(ptime const& dt)
    {
        return std::make_shared<arrow::TimestampScalar>(fromPTime(dt),
                                                        arrow::TimeUnit::NANO);
    }

    std::array<std::shared_ptr<arrow::Array>, 3>
    getTimeBins( std::shared_ptr<arrow::Array> const& ax);

    pd::DataFrame concat(std::vector<pd::DataFrame> const& objs,
                         AxisType axis,
                         JoinType join,
                         bool ignore_index,
                         bool sort);

    pd::DataFrame concatRows(std::vector<pd::DataFrame> const& objs,
                             JoinType join=pd::JoinType::Outer,
                             bool ignore_index=false,
                             bool sort=false);

    pd::DataFrame concatColumns(std::vector<pd::DataFrame> const& objs,
                             JoinType join=pd::JoinType::Outer,
                             bool ignore_index=false,
                             bool sort=false);

    pd::DataFrame concatColumnsUnsafe(std::vector<pd::DataFrame> const& objs);

    std::vector<std::shared_ptr<arrow::Field> >
    mergeColumns(std::vector<pd::DataFrame> const& objs,
                 JoinType join);

    std::shared_ptr<arrow::Array>
    mergeRows(std::vector<pd::DataFrame> const& objs,
                 JoinType join);

    std::shared_ptr<arrow::Array>
    combineIndexes(std::vector<std::shared_ptr<arrow::Array> > const& indexes,
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
            auto double_builder =
                std::make_shared<typename arrow::CTypeTraits<T>::BuilderType>();
            double_builder->AppendValues(x);
            return double_builder->Finish().MoveValueUnsafe();
        }

        static auto Make(std::vector<T> const& x, std::vector<bool> const& map)
        {
            auto double_builder =
                std::make_shared<typename arrow::CTypeTraits<T>::BuilderType>();
            double_builder->AppendValues(x, map);
            return double_builder->Finish().MoveValueUnsafe();
        }
};

struct DateArray : ArrayT<date>
{
        static auto Make(std::vector<date> const& x)
        {
            TimestampBuilder builder(
                std::make_shared<arrow::TimestampType>(arrow::TimeUnit::NANO),
                default_memory_pool());

            builder.Reserve(x.size());
            for (auto const& t : x)
            {
                builder.Append(pd::fromDate(t));
            }
            return builder.Finish().MoveValueUnsafe();
        }

        static auto Make(std::vector<date> const& x, std::vector<bool> const& map)
        {
            TimestampBuilder builder(
                std::make_shared<arrow::TimestampType>(arrow::TimeUnit::NANO),
                default_memory_pool());

            std::vector<int64_t> ts(x.size());
            std::ranges::transform(
                x,
                ts.begin(),
                [](auto const& t) { return pd::fromDate(t); });

            builder.AppendValues(ts, map);

            return builder.Finish().MoveValueUnsafe();
        }
};

struct DateTimeArray : ArrayT<ptime>
{
        static auto Make(std::vector<ptime> const& x)
        {
            TimestampBuilder builder(
                std::make_shared<arrow::TimestampType>(arrow::TimeUnit::NANO),
                default_memory_pool());

            builder.Reserve(x.size());
            for (auto const& t : x)
            {
                builder.Append(pd::fromPTime(t));
            }
            return builder.Finish().MoveValueUnsafe();
        }

        static auto Make(std::vector<ptime> const& x, std::vector<bool> const& map)
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

            return builder.Finish().MoveValueUnsafe();
        }
};
}