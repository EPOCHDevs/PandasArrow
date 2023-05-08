#pragma once
//
// Created by dewe on 1/21/23.
//

#include "group_by.h"

namespace pd {

struct GroupInfo
{
    std::vector<int64_t> bins;
    std::shared_ptr<arrow::TimestampArray> labels;

    bool upsampling() const
    {
        return bins.back() < labels->length();
    }

    inline std::vector<int64_t> downsample()
    {

        if (bins.size() != labels->length())
        {
            throw std::runtime_error("Processing Group Info requires bins.size() == labels->length()");
        }

        if (bins.empty())
        {
            return {};
        }

        std::vector<int64_t> timestamps_ns;
        timestamps_ns.resize(bins.back());

        int64_t last_idx = 0;
        auto label = labels->raw_values();
        for (int64_t bin : bins)
        {
            std::fill(timestamps_ns.begin() + last_idx, timestamps_ns.begin() + bin, *label);
            label++;
            last_idx = bin;
        }
        return timestamps_ns;
    }
};

template<class T>
std::vector<int64_t> generate_bins_dt64(
    std::shared_ptr<T> timestamps_array,
    std::shared_ptr<T> const& bin_edges,
    bool closed_right);

extern template std::vector<int64_t> generate_bins_dt64(
    std::shared_ptr<arrow::TimestampArray> values,
    std::shared_ptr<arrow::TimestampArray> const& binner,
    bool closed_right);

extern template std::vector<int64_t> generate_bins_dt64(
    std::shared_ptr<arrow::Int64Array> values,
    std::shared_ptr<arrow::Int64Array> const& binner,
    bool closed_right);

extern template std::vector<int64_t> generate_bins_dt64(
    std::shared_ptr<arrow::UInt64Array> values,
    std::shared_ptr<arrow::UInt64Array> const& binner,
    bool closed_right);

template<class DataFrameOrSeries>
Resampler resample(
    DataFrameOrSeries const& df,
    std::string const& rule,
    bool closed_right = false,
    bool label_right = false,
    std::variant<ptime, TimeGrouperOrigin> const& origin = TimeGrouperOrigin::StartDay,
    time_duration const& offset = time_duration(),
    std::string const& tz = "")
{
    auto [freq_unit, freq_value] = splitTimeSpan(rule);
    std::variant<DateOffset, time_duration> duration{};
    if (freq_unit == "T" or freq_unit == "min")
    {
        duration = minutes(freq_value);
    }
    else if (freq_unit == "S")
    {
        duration = seconds(freq_value);
    }
    else if (freq_unit == "L" or freq_unit == "ms")
    {
        duration = milliseconds(freq_value);
    }
    else if (freq_unit == "U" or freq_unit == "us")
    {
        duration = microseconds(freq_value);
    }
    else if (freq_unit == "N" or freq_unit == "ns")
    {
        duration = nanoseconds(freq_value);
    }
    else
    {
        duration = DateOffset::FromString(rule);
    }

    return resample(df, duration, closed_right, label_right, origin, offset, tz);
}

template<class DataFrameOrSeries>
Resampler resample(
    DataFrameOrSeries const& df,
    std::variant<DateOffset, time_duration> const& rule,
    bool closed_right = false,
    bool label_right = false,
    std::variant<ptime, TimeGrouperOrigin> const& origin = TimeGrouperOrigin::StartDay,
    time_duration const& offset = time_duration(),
    std::string const& tz = "")
{
    GroupInfo group_info = makeGroupInfo(df.indexArray(), rule, closed_right, label_right, origin, offset, tz);

    bool reindex = group_info.upsampling();
    auto new_index = group_info.upsampling() ? group_info.labels : toDateTime(group_info.downsample());

    DataFrame new_df{ nullptr, nullptr };
    if constexpr (std::same_as<DataFrameOrSeries, Series>)
    {
        new_df = DataFrame{ arrow::schema({ arrow::field(df.name(), df.dtype()) }),
                            new_index->length(),
                            { df.array() },
                            df.indexArray() };
    }
    else
    {
        new_df = df;
    }

    new_df = reindex ? new_df.reindex(new_index) : new_df.setIndex(new_index);
    return { new_df };
}

arrow::TimestampArray adjustBinEdges(arrow::TimestampArray& binner, arrow::TimestampArray const& ax_values);

GroupInfo makeGroupInfo(
    std::shared_ptr<arrow::Array> const& timestamps_array,
    std::variant<DateOffset, time_duration> const& rule,
    bool closed_right = false,
    bool label_right = false,
    std::variant<ptime, TimeGrouperOrigin> const& origin = TimeGrouperOrigin::StartDay,
    time_duration const& offset = time_duration(),
    std::string const& tz = "");

std::pair<ptime, ptime> adjustDatesAnchored(
    ptime const& start,
    ptime const& end,
    time_duration const& freq,
    bool closed_right = false,
    std::variant<ptime, TimeGrouperOrigin> const& origin = TimeGrouperOrigin::StartDay,
    time_duration const& offset = time_duration(),
    std::string const& tz = "");

std::pair<ptime, ptime> getTimestampRangeEdges(
    ptime const& first,
    ptime const& last,
    time_duration const& freq,
    bool closed_right,
    std::variant<ptime, TimeGrouperOrigin> const& origin,
    time_duration const& offset);

} // namespace pd