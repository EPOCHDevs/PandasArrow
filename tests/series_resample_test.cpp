//
// Created by adesola on 1/11/25.
//
#include <catch.hpp>
#include "pandas_arrow.h"


using namespace std::string_literals;
using namespace Catch;
using namespace pd;

TEST_CASE("Test resample on series", "[ResampleSeries]")
{
    auto index = pd::date_range(ptime(date(2000, 1, 1)), 9);
    auto series = pd::Series(pd::range(0L, 9L), index);

    SECTION("Downsample series into 3 minute bins  and sum")
    {
        auto resampler = pd::resample(series, time_duration(0, 3, 0));

        auto group_index = resampler.index();
        REQUIRE(pd::ReturnOrThrowOnFailure(group_index->GetScalar(0))->ToString() == "2000-01-01 00:00:00.000000000");
        REQUIRE(pd::ReturnOrThrowOnFailure(group_index->GetScalar(1))->ToString() == "2000-01-01 00:03:00.000000000");
        REQUIRE(pd::ReturnOrThrowOnFailure(group_index->GetScalar(2))->ToString() == "2000-01-01 00:06:00.000000000");

        auto sum = pd::ReturnOrThrowOnFailure(resampler.sum());
        REQUIRE(sum.at(0, 0) == 3L);
        REQUIRE(sum.at(1, 0) == 12L);
        REQUIRE(sum.at(2, 0) == 21L);
    }

    SECTION(
            "Downsample series into 3 minute bins  and sum, "
            "label with right")
    {
        auto resampler = pd::resample(series, time_duration(0, 3, 0), false, true);

        auto group_index = resampler.index();
        REQUIRE(pd::ReturnOrThrowOnFailure(group_index->GetScalar(0))->ToString() == "2000-01-01 00:03:00.000000000");
        REQUIRE(pd::ReturnOrThrowOnFailure(group_index->GetScalar(1))->ToString() == "2000-01-01 00:06:00.000000000");
        REQUIRE(pd::ReturnOrThrowOnFailure(group_index->GetScalar(2))->ToString() == "2000-01-01 00:09:00.000000000");

        auto sum = pd::ReturnOrThrowOnFailure(resampler.sum());
        REQUIRE(sum.at(0, 0) == 3L);
        REQUIRE(sum.at(1, 0) == 12L);
        REQUIRE(sum.at(2, 0) == 21L);
    }

    SECTION(
            "Downsample series into 3 minute bins  and sum, "
            "label and close right")
    {
        auto resampler = pd::resample(series, time_duration(0, 3, 0), true, true);

        auto group_index = resampler.index();

        REQUIRE(group_index->length() == 4);

        REQUIRE(pd::ReturnOrThrowOnFailure(group_index->GetScalar(0))->ToString() == "2000-01-01 00:00:00.000000000");
        REQUIRE(pd::ReturnOrThrowOnFailure(group_index->GetScalar(1))->ToString() == "2000-01-01 00:03:00.000000000");
        REQUIRE(pd::ReturnOrThrowOnFailure(group_index->GetScalar(2))->ToString() == "2000-01-01 00:06:00.000000000");
        REQUIRE(pd::ReturnOrThrowOnFailure(group_index->GetScalar(3))->ToString() == "2000-01-01 00:09:00.000000000");

        auto sum = pd::ReturnOrThrowOnFailure(resampler.sum());
        REQUIRE(sum.at(0, 0) == 0L);
        REQUIRE(sum.at(1, 0) == 6L);
        REQUIRE(sum.at(2, 0) == 15L);
        REQUIRE(sum.at(3, 0) == 15L);
    }
}

TEST_CASE("Test resample on series with custom function", "[ResampleSeries]")
{
    auto index = pd::date_range(ptime(date(2000, 1, 1)), 9);
    auto series = pd::Series(pd::range(0L, 9L), index);

    auto custom_resampler = [](DataFrame const& df) { return (df.sum() + 5).scalar; };

    pd::Series result = pd::ReturnOrThrowOnFailure(series.resample("3T").apply(custom_resampler));

    REQUIRE(result.size() == 3);
    REQUIRE(result[0] == 8L);
    REQUIRE(result[1] == 17L);
    REQUIRE(result[2] == 26L);
}

TEST_CASE("More resample test on series", "[Resample]") {
    auto index = pd::date_range(ptime(date(2000, 1, 1)), 9);
    auto df = pd::Series(pd::range(0L, 9L), index).toFrame("i");

    SECTION("Downsample series into 3 minute bins  and sum, closed left, label left") {
        auto resampler = df.downsample("3T", false);

        auto group_index = resampler.index();
        auto sum = pd::ReturnOrThrowOnFailure(resampler.sum());

        INFO(sum);

        REQUIRE(group_index->length() == 3);
        REQUIRE(pd::ReturnOrThrowOnFailure(group_index->GetScalar(0))->ToString() == "2000-01-01 00:00:00.000000000");
        REQUIRE(pd::ReturnOrThrowOnFailure(group_index->GetScalar(1))->ToString() == "2000-01-01 00:03:00.000000000");
        REQUIRE(pd::ReturnOrThrowOnFailure(group_index->GetScalar(2))->ToString() == "2000-01-01 00:06:00.000000000");

        REQUIRE(sum.at(0, 0) == 3L);
        REQUIRE(sum.at(1, 0) == 12L);
        REQUIRE(sum.at(2, 0) == 21L);
    }

    SECTION(
            "Downsample series into 3 minute bins  and sum, "
            "close right and label right") {
        auto resampler = df.downsample("3T", true);

        auto group_index = resampler.index();
        auto sum = pd::ReturnOrThrowOnFailure(resampler.sum());

        INFO(sum);

        REQUIRE(group_index->length() == 4);

        REQUIRE(pd::ReturnOrThrowOnFailure(group_index->GetScalar(0))->ToString() == "2000-01-01 00:00:00.000000000");
        REQUIRE(pd::ReturnOrThrowOnFailure(group_index->GetScalar(1))->ToString() == "2000-01-01 00:03:00.000000000");
        REQUIRE(pd::ReturnOrThrowOnFailure(group_index->GetScalar(2))->ToString() == "2000-01-01 00:06:00.000000000");
        REQUIRE(pd::ReturnOrThrowOnFailure(group_index->GetScalar(3))->ToString() == "2000-01-01 00:09:00.000000000");

        REQUIRE(sum.at(0, 0) == 0L);
        REQUIRE(sum.at(1, 0) == 6L);
        REQUIRE(sum.at(2, 0) == 15L);
        REQUIRE(sum.at(3, 0) == 15L);
    }

    SECTION("Test resample on series with custom function", "[Resample]") {
        auto custom_resampler = [](pd::DataFrame const &df) { return (df.sum() + 5).scalar; };

        const pd::Series result = pd::ReturnOrThrowOnFailure(df.downsample("3T", false).apply(custom_resampler));
        INFO(result);

        REQUIRE(result.size() == 3);
        REQUIRE(result[0] == 8L);
        REQUIRE(result[1] == 17L);
        REQUIRE(result[2] == 26L);
    }

    SECTION("Week Resampling", "[Resample]") {
        auto week_starting = arrow::DateArray::Make(std::vector{
                date(2018, Jan, 7),
                date(2018, Jan, 14),
                date(2018, Jan, 21),
                date(2018, Jan, 28),
                date(2018, Feb, 4),
                date(2018, Feb, 11),
                date(2018, Feb, 18),
                date(2018, Feb, 25),
        });
        auto week_df = pd::DataFrame(week_starting, pd::GetRow("price", {10, 11, 9, 13, 14, 18, 17, 19}), pd::GetRow("volume", {50, 60, 40, 100, 50, 100, 40, 50}));

        const pd::DataFrame result = pd::ReturnOrThrowOnFailure(week_df.downsample("M").mean());
        INFO(result);

        auto group_index = result.indexArray();
        REQUIRE(pd::ReturnOrThrowOnFailure(group_index->GetScalar(0))->ToString() == "2018-01-31 00:00:00.000000000");
        REQUIRE(pd::ReturnOrThrowOnFailure(group_index->GetScalar(1))->ToString() == "2018-02-28 00:00:00.000000000");

        REQUIRE(result["price"].values<double>() == std::vector<double>{10.75, 17});
        REQUIRE(result["volume"].values<double>() == std::vector<double>{62.5, 60});
    }
}

//TEST_CASE("Upsample the series into 30 second bins.")
//{
//    auto index = pd::date_range(ptime(date(2000, 1, 1)), 9);
//    auto series = pd::Series(pd::range(0L, 9L), index);
//
//    auto resampler = pd::resample(series, time_duration(0, 0, 30));
//
//    auto group_index = resampler.index();
//
//    REQUIRE(group_index->length() == 17);
//
//    REQUIRE(pd::ReturnOrThrowOnFailure(group_index->GetScalar(0))->ToString() == "2000-01-01 00:00:00.000000000");
//    REQUIRE(pd::ReturnOrThrowOnFailure(group_index->GetScalar(1))->ToString() == "2000-01-01 00:00:30.000000000");
//    REQUIRE(pd::ReturnOrThrowOnFailure(group_index->GetScalar(2))->ToString() == "2000-01-01 00:01:00.000000000");
//    REQUIRE(pd::ReturnOrThrowOnFailure(group_index->GetScalar(3))->ToString() == "2000-01-01 00:01:30.000000000");
//    REQUIRE(pd::ReturnOrThrowOnFailure(group_index->GetScalar(4))->ToString() == "2000-01-01 00:02:00.000000000");
//
//    auto df = resampler.data();
//    REQUIRE(df.num_rows() == 17);
//
//    REQUIRE(df.at(0, 0) == 0L);
//    REQUIRE_FALSE(df.at(1, 0).isValid());
//    REQUIRE(df.at(2, 0) == 1l);
//    REQUIRE_FALSE(df.at(3, 0).isValid());
//    REQUIRE(df.at(4, 0) == 2l);
//
//    df = df.ffill();
//    INFO(df);
//    REQUIRE(df.at(0, 0) == 0L);
//    REQUIRE(df.at(1, 0) == 0L);
//    REQUIRE(df.at(2, 0) == 1l);
//    REQUIRE(df.at(3, 0) == 1l);
//    REQUIRE(df.at(4, 0) == 2l);
//}