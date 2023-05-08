//
// Created by dewe on 1/21/23.
//
#include "resample.h"
#include "arrow/compute/api.h"
#include "group_by.h"


namespace pd {

template<class T>
std::vector<int64_t> generate_bins_dt64(std::shared_ptr<T> values, std::shared_ptr<T> const& binner, bool right_closed)
{

    int64_t nat_count = 0;
    if (values->null_count() > 0)
    {
        auto datum = pd::ReturnOrThrowOnFailure(arrow::compute::DropNull(values));
        values = datum.template array_as<T>();
        nat_count = values->null_count();
    }

    std::vector<int64_t> bins;

    int64_t values_len = values->length();
    int64_t binner_len = binner->length();
    bins.reserve(binner_len);

    if (values_len <= 0 || binner_len <= 0)
    {
        throw std::invalid_argument("Invalid length for values or for binner");
    }

    if (values->Value(0) < binner->Value(0))
    {
        throw std::invalid_argument("Values falls before first bin");
    }

    if (values->Value(values_len - 1) > binner->Value(binner_len - 1))
    {
        throw std::invalid_argument("Values falls after last bin");
    }

    int64_t j = 0;
    int64_t bc = 0;

    if (right_closed)
    {
        for (int64_t i = 0; i < binner_len - 1; i++)
        {
            int64_t r_bin = binner->Value(i + 1);
            // count values in current bin, advance to next bin
            while (j < values_len && values->Value(j) <= r_bin)
            {
                j++;
            }
            bins.push_back(j);
            bc++;
        }
    }
    else
    {
        for (int64_t i = 0; i < binner_len - 1; i++)
        {
            int64_t r_bin = binner->Value(i + 1);
            // count values in current bin, advance to next bin
            while (j < values_len && values->Value(j) < r_bin)
            {
                j++;
            }
            bins.push_back(j);
            bc++;
        }
    }

    if (values->null_count() > 0)
    {
        // shift bins by the number of NaT
        for (auto& bin : bins)
        {
            bin += values->null_count();
        }
        bins.insert(bins.begin(), values->null_count(), 0);
    }

    return bins;
}

template std::vector<int64_t> generate_bins_dt64(
    std::shared_ptr<arrow::TimestampArray> values,
    std::shared_ptr<arrow::TimestampArray> const& binner,
    bool closed_right);

template std::vector<int64_t> generate_bins_dt64(
    std::shared_ptr<arrow::Int64Array> values,
    std::shared_ptr<arrow::Int64Array> const& binner,
    bool closed_right);

template std::vector<int64_t> generate_bins_dt64(
    std::shared_ptr<arrow::UInt64Array> values,
    std::shared_ptr<arrow::UInt64Array> const& binner,
    bool closed_right);

std::pair<ptime, ptime> adjustDatesAnchored(
    ptime const& start,
    ptime const& end,
    time_duration const& freq,
    bool closed_right,
    std::variant<ptime, TimeGrouperOrigin> const& origin_opt,
    time_duration const& offset,
    std::string const& tz)
{
    // First and last offsets should be calculated from the start day to fix an
    // error cause by resampling across multiple days when a one day period is
    // not a multiple of the frequency.
    ptime first = start;
    ptime last = end;

    ptime origin_time(date(1970, 1, 1));
    ; // origin == "epoch"
    if (std::holds_alternative<TimeGrouperOrigin>(origin_opt))
    {
        auto origin = std::get<1>(origin_opt);
        if (origin == TimeGrouperOrigin::StartDay)
        {
            origin_time = ptime(first.date());
        }
        else if (origin == TimeGrouperOrigin::Start)
        {
            origin_time = first;
        }
        else if (origin == TimeGrouperOrigin::End)
        {
            origin_time = last;
        }
        else if (origin == TimeGrouperOrigin::EndDay)
        {
            origin_time = ptime(last.date());
        }
    }
    else
    {
        origin_time = std::get<0>(origin_opt);
    }

    origin_time += offset;

    if (not tz.empty())
    {
        time_t first_time = to_time_t(first);
        time_t last_time = to_time_t(last);

        tm* first_tm = gmtime(&first_time);
        first = from_time_t(mktime(first_tm));

        tm* last_tm = gmtime(&last_time);
        last = from_time_t(mktime(last_tm));
    }

    time_duration foffset(nanosec((first - origin_time).total_nanoseconds() % freq.total_nanoseconds()));
    time_duration loffset(nanoseconds((last - origin_time).total_nanoseconds() % freq.total_nanoseconds()));

    if (closed_right)
    {
        if (foffset > time_duration())
        {
            // roll back
            first -= foffset;
        }
        else
        {
            first -= freq;
        }

        if (loffset > time_duration())
        {
            // roll forward
            last += (freq - loffset);
        }
        else
        {
            // already the end of the road
            last = last;
        }
    }
    else
    { // closed == 'left'
        if (foffset > time_duration())
        {
            first -= foffset;
        }
        else
        {
            // start of the road
            first = first;
        }

        if (loffset > time_duration())
        {
            // roll forward
            last += (freq - loffset);
        }
        else
        {
            last += freq;
        }
    }

    return std::make_pair(first, last);
}

GroupInfo makeGroupInfo(
    std::shared_ptr<arrow::Array> const& ax,
    std::variant<DateOffset, time_duration> const& rule,
    bool closed_right,
    bool label_right,
    std::variant<ptime, TimeGrouperOrigin> const& origin,
    time_duration const& offset,
    std::string const& tz)
{
    auto timestamps_ax = std::dynamic_pointer_cast<arrow::TimestampArray>(ax);

    if (not timestamps_ax)
    {
        throw std::runtime_error("axis must be a TimestampArray but got array of type " + ax->type()->ToString());
    }

    if (timestamps_ax->length() == 0)
    {
        return { {}, timestamps_ax };
    }

    ASSIGN_OR_ABORT(auto datum, arrow::compute::MinMax(timestamps_ax));
    auto datum_struct = datum.scalar_as<arrow::StructScalar>();
    auto [min, max] = MinMax{ datum_struct.value[0], datum_struct.value[1] };

    ptime minValue = toTimeNanoSecPtime(min.scalar);
    ptime maxValue = toTimeNanoSecPtime(max.scalar);

    std::shared_ptr<arrow::NumericArray<arrow::TimestampType>> binner;
    if (std::holds_alternative<time_duration>(rule))
    {
        const auto duration = std::get<time_duration>(rule);
        auto [first, last] = adjustDatesAnchored(
            minValue,
            maxValue,
            duration,
            closed_right,
            origin,
            offset,
            tz);
        binner = date_range(first, last, duration, tz);
    }
    else
    {
        date first = minValue.date();
        date last = maxValue.date();

        const auto freq = std::get<DateOffset>(rule);

        if (not closed_right)
        {
            throw std::runtime_error("closed_left is not currently supported by DateOffset");
        }

        binner = date_range(first - freq, last + freq, freq, tz);
    }

    auto labels = binner;

    auto bins = generate_bins_dt64(timestamps_ax, binner, closed_right);

    // Handle the closed_right and label_right options
    if (closed_right)
    {
        labels = binner;
        if (label_right)
        {
            labels = std::dynamic_pointer_cast<arrow::TimestampArray>(labels->Slice(1));
        }
    }
    else if (label_right)
    {
        labels = std::dynamic_pointer_cast<arrow::TimestampArray>(labels->Slice(1));
    }

    if (ax->null_count() > 0)
    {
        labels = dynamic_pointer_cast<arrow::TimestampArray>(
            arrow::Concatenate({ arrow::MakeArrayOfNull(labels->type(), 1).MoveValueUnsafe(), labels })
                .MoveValueUnsafe());
    }

    if (bins.size() < labels->length())
    {
        labels = dynamic_pointer_cast<arrow::TimestampArray>(labels->Slice(0, bins.size()));
    }

    return { bins, labels };
}

} // namespace pd