//
// Created by dewe on 1/21/23.
//
#include "resample.h"
#include "arrow/compute/api.h"
#include "group_by.h"


namespace pd {

std::vector<int64_t> generate_bins_dt64(std::shared_ptr<arrow::TimestampArray> values,
                                        std::shared_ptr<arrow::Int64Array> const& binner,
                                        bool right_closed)
{
    int64_t nat_count = 0;
    if (values->null_count() > 0) {
        auto datum = pd::ReturnOrThrowOnFailure(arrow::compute::DropNull(values));
        values = datum.array_as<arrow::TimestampArray>();
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

std::pair<ptime, ptime> adjustDatesAnchored(
    ptime const& start,
    ptime const& end,
    time_duration const& freq,
    bool closed_right,
    TimeGrouperOrigin const& origin,
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

    if (origin.type == TimeGrouperOrigin::StartDay)
    {
        origin_time = ptime(first.date());
    }
    else if (origin.type == TimeGrouperOrigin::Start)
    {
        origin_time = first;
    }
    else if (origin.type == TimeGrouperOrigin::End)
    {
        origin_time = last;
    }
    else if (origin.type == TimeGrouperOrigin::EndDay)
    {
        origin_time = ptime(last.date());
    }
    else
    {
        origin_time = origin.custom;
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

    std::shared_ptr<arrow::Int64Array> adjustBinEdges(std::shared_ptr<arrow::TimestampArray>& binner,
                                                      int64_t max_timestamp) {

        auto binEdges = pd::ReturnOrThrowOnFailure(arrow::compute::Add(binner,
                                                                       std::make_shared<arrow::DurationScalar>(
                                                                               60 * 60 * 24/*1D*/,
                                                                               arrow::TimeUnit::SECOND)));
        binEdges = pd::ReturnOrThrowOnFailure(arrow::compute::Subtract(binEdges,
                                                                       std::make_shared<arrow::DurationScalar>(1,
                                                                                                               arrow::TimeUnit::NANO)));
        binEdges = pd::ReturnOrThrowOnFailure(arrow::compute::Cast(binEdges, arrow::int64()));

        auto binEdgesArray = binEdges.array_as<arrow::Int64Array>();
        const int64_t length = binEdgesArray->length();

        if (binEdgesArray->Value(length - 2) > max_timestamp) {
            binner = static_pointer_cast<arrow::TimestampArray>(binner->Slice(0, length - 1));
            binEdgesArray = static_pointer_cast<arrow::Int64Array>(binEdgesArray->Slice(0, length - 1));
        }
        return binEdgesArray;
    }

GroupInfo makeGroupInfo(
    std::shared_ptr<arrow::Array> const& ax,
    std::variant<DateOffset, time_duration> const& rule,
    bool closed_right,
    bool label_right,
    TimeGrouperOrigin const& origin,
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

    const auto datum = pd::ReturnOrThrowOnFailure(arrow::compute::MinMax(timestamps_ax));
    auto datum_struct = datum.scalar_as<arrow::StructScalar>();
    auto [min, max] = MinMax{ datum_struct.value[0], datum_struct.value[1] };

    ptime minValue = toTimeNanoSecPtime(min.scalar);
    ptime maxValue = toTimeNanoSecPtime(max.scalar);

    std::shared_ptr<arrow::TimestampArray> binner, labels;
    std::shared_ptr<arrow::Int64Array> binEdges;

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
        labels = binner;
        binEdges = pd::ReturnOrThrowOnFailure(arrow::compute::Cast(binner, arrow::int64())).array_as<arrow::Int64Array>();
    }
    else {
        date first = minValue.date();
        date last = maxValue.date();

        const auto freq = std::get<DateOffset>(rule);

        if (not closed_right) {
            throw std::runtime_error("closed_left is not currently supported by DateOffset");
        }

        binner = date_range(first - freq, last + freq, freq, tz);
        if ((freq.type == DateOffset::Day && freq.multiplier > 1) or freq.type != DateOffset::Day) {
            binEdges = adjustBinEdges(binner, pd::fromPTime(maxValue));
        } else {
            binEdges = pd::ReturnOrThrowOnFailure(
                    arrow::compute::Cast(binner, arrow::int64())).array_as<arrow::Int64Array>();
        }
    }

    auto bins = generate_bins_dt64(timestamps_ax, binEdges, closed_right);
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