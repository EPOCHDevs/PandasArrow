//
// Created by adesola on 1/11/25.
//
#include "pandas_arrow.h"
#include <catch.hpp>


using namespace std::string_literals;
using namespace Catch;
using namespace pd;

TEST_CASE("Test Series::where() function") {
    std::vector<int> vec1 = {1, 2, 3, 4, 5};
    auto array1 = arrow::ArrayT<int>::Make(vec1);
    Series s1(array1, true);

    std::vector<bool> mask = {true, false, true, false, true};
    auto array2 = arrow::ArrayT<bool>::Make(mask);
    Series mask_series(array2, true);

    SECTION("Test where() with a mask array") {
        REQUIRE_THROWS_AS(s1.where(mask_series), std::runtime_error);
    }

    SECTION("Test where() with a mask array and a series of different size") {
        std::vector<int> vec1 = {1, 2, 3, 4, 5};
        Series s1(vec1);

        std::vector<bool> mask = {false, true, true};
        Series mask_s(mask);

        REQUIRE_THROWS_AS(s1.where(mask_s), std::runtime_error);
    }

    SECTION("Test where() with a mask array and a scalar value") {
        std::vector<int> vec1 = {1, 2, 3, 4, 5};
        Series s1(vec1);

        std::vector<bool> mask = {false, true, true, true, false};
        Series mask_s(mask);

        Scalar scalar(3);
        Series result = s1.where(mask_s, scalar);

        REQUIRE(result.size() == 5);
        REQUIRE(result.at(0) == 3);
        REQUIRE(result.at(1) == 2);
        REQUIRE(result.at(2) == 3);
        REQUIRE(result.at(3) == 4);
        REQUIRE(result.at(4) == 3);
    }
}

TEST_CASE("Test Series::take() function") {
    std::vector<int> vec1 = {1, 2, 3, 4, 5};
    Series s1(vec1);

    std::vector<bool> mask = {false, true, true, true, false};
    Series mask_s(mask);

    SECTION("Test take() with a mask array") {
        REQUIRE_THROWS_AS(s1.take(mask_s), std::runtime_error);
    }

    SECTION("Test take() with a index array") {
        std::vector<int32_t> index = {1, 3, 4};
        Series result{s1.take(Series{index})};

        REQUIRE(result.size() == 3);
        REQUIRE(result.at(0) == 2);
        REQUIRE(result.at(1) == 4);
        REQUIRE(result.at(2) == 5);
    }
}

TEST_CASE("Test Series::operator[] (slice) function") {
    std::vector<int> vec1{1, 2, 3, 4, 5};
    auto array1 = arrow::ArrayT<int>::Make(vec1);
    Series s1(array1, false);

    // Test positive index slice
    SECTION("Test positive index slice") {
        auto result = s1[Slice{1, 3}];
        REQUIRE(result.size() == 2);
        REQUIRE(result.at(0) == 2);
        REQUIRE(result.at(1) == 3);
    }

    // Test negative index slice
    SECTION("Test negative index slice") {
        auto result = s1[Slice{-3, -1}];
        REQUIRE(result.size() == 2);
        REQUIRE(result.at(0) == 3);
        REQUIRE(result.at(1) == 4);
    }

    // Test slice with negative and positive index
    SECTION("Test slice with negative and positive index") {
        auto result = s1[Slice{-3, 4}];
        REQUIRE(result.size() == 2);
        REQUIRE(result.at(0) == 3);
        REQUIRE(result.at(1) == 4);
    }

    // Test slice with out of bounds index
    SECTION("Test slice with out of bounds index") {
        REQUIRE_THROWS_AS((s1[Slice{-6, -1}]), std::runtime_error);
        REQUIRE_THROWS_AS((s1[Slice{6, 10}]), std::runtime_error);
    }
}

TEST_CASE("Test Series::operator with dateslice") {
    std::vector<int> vec1 = {1, 2, 3, 4, 5};
    std::vector<date> vec2 = {date(2022, 1, 1),
                              date(2022, 1, 2),
                              date(2022, 1, 3),
                              date(2022, 1, 4),
                              date(2022, 1, 5)};
    auto array1 = arrow::ArrayT<int>::Make(vec1);
    auto index = arrow::DateArray::Make(vec2);
    Series s1(array1, index);

    // Test with valid slicer
    DateSlice slicer1 = {date(2022, 1, 2), date(2022, 1, 4)};

    auto result1 = s1[slicer1];
    REQUIRE(result1.size() == 2);
    REQUIRE(result1.at(0) == 2);
    REQUIRE(result1.at(1) == 3);

    // Test with only start
    DateSlice slicer2 = {date(2022, 1, 3)};
    auto result2 = s1[slicer2];
    REQUIRE(result2.size() == 3);
    REQUIRE(result2.at(0) == 3);
    REQUIRE(result2.at(1) == 4);
    REQUIRE(result2.at(2) == 5);

    // Test with only end
    DateSlice slicer3 = {std::nullopt, date(2022, 1, 3)};
    auto result3 = s1[slicer3];
    REQUIRE(result3.size() == 2);
    REQUIRE(result3.at(0) == 1);
    REQUIRE(result3.at(1) == 2);

    // Test with both start and end
    DateSlice slicer4 = {date(2022, 1, 2), date(2022, 1, 7)};
    auto result4= s1[slicer4];
    REQUIRE(result4.size() == 4);

    DateSlice slicer5 = {date(2022, 1, 6), date(2022, 1, 7)};
    REQUIRE(s1[slicer5].size() == 0);
}

TEST_CASE("Test Series::operator with DateTimeSlice") {
    // Create sample data
    std::vector<int> vec1 = {10, 20, 30, 40, 50};
    std::vector<ptime> vec2 = {
            ptime(date(2022, 1, 1), hours(10)),
            ptime(date(2022, 1, 1), hours(12)),
            ptime(date(2022, 1, 1), hours(14)),
            ptime(date(2022, 1, 2), hours(10)),
            ptime(date(2022, 1, 2), hours(12))
    };

    auto array1 = arrow::ArrayT<int>::Make(vec1);
    auto index = arrow::DateTimeArray::Make(vec2);
    Series s1(array1, index);

    SECTION("Test with valid slicer") {
        DateTimeSlice slicer1 = {ptime(date(2022, 1, 1), hours(12)), ptime(date(2022, 1, 2), hours(10))};
        auto result1 = s1[slicer1];
        REQUIRE(result1.size() == 2);
        REQUIRE(result1.at(0) == 20);
        REQUIRE(result1.at(1) == 30);
    }

    SECTION("Test with only start time") {
        DateTimeSlice slicer2 = {ptime(date(2022, 1, 1), hours(14)), std::nullopt};
        auto result2 = s1[slicer2];
        REQUIRE(result2.size() == 3);
        REQUIRE(result2.at(0) == 30);
        REQUIRE(result2.at(1) == 40);
        REQUIRE(result2.at(2) == 50);
    }

    SECTION("Test with only end time") {
        DateTimeSlice slicer3 = {std::nullopt, ptime(date(2022, 1, 1), hours(14))};
        auto result3 = s1[slicer3];
        REQUIRE(result3.size() == 2);
        REQUIRE(result3.at(0) == 10);
        REQUIRE(result3.at(1) == 20);
    }

    SECTION("Test with only end time , LOC") {
        DateTimeSlice slicer3 = {std::nullopt, ptime(date(2022, 1, 1), hours(14))};
        auto result3 = s1.loc(slicer3);
        REQUIRE(result3.size() == 3);
        REQUIRE(result3.at(0) == 10);
        REQUIRE(result3.at(1) == 20);
        REQUIRE(result3.at(2) == 30);
    }

    SECTION("Test with invalid range") {
        DateTimeSlice slicer4 = {ptime(date(2022, 1, 2), hours(14)), ptime(date(2022, 1, 1), hours(10))};
        REQUIRE(s1[slicer4].size() == 0);
    }
}


TEST_CASE("Test Series::operator[] with StringSlice") {
    std::vector<int> vec1 = {1, 2, 3, 4, 5};
    auto array1 = arrow::ArrayT<int>::Make(vec1);

    std::vector<std::string> vec2 = {"a", "b", "c", "d", "e"};
    auto index = arrow::ArrayT<std::string>::Make(vec2);

    Series s1(array1, index);

    // Test with only start
    StringSlice slicer1 = {std::string("c"), std::nullopt};
    auto result1 = s1[slicer1];
    REQUIRE(result1.size() == 3);
    REQUIRE(result1.at(0) == 3);
    REQUIRE(result1.at(1) == 4);
    REQUIRE(result1.at(2) == 5);

    // Test with only end
    StringSlice slicer2 = {std::nullopt, std::string("c")};
    auto result2 = s1[slicer2];
    REQUIRE(result2.size() == 2);
    REQUIRE(result2.at(0) == 1);
    REQUIRE(result2.at(1) == 2);

    // Test with both start and end
    StringSlice slicer3 = {std::string("b"), std::string("d")};
    auto result3 = s1[slicer3];
    REQUIRE(result3.size() == 2);
    REQUIRE(result3.at(0) == 2);
    REQUIRE(result3.at(1) == 3);

    REQUIRE_THROWS_AS((s1[StringSlice{std::string("b"), std::string("g")}]), std::runtime_error);

    REQUIRE_THROWS_AS((s1[StringSlice{std::string("k"), std::string("b")}]), std::runtime_error);
}

TEST_CASE("Test DateTimeLike", "[core]") {
    // Create a test vector of DateTime values
    auto test_vec = std::vector<std::string>{"2022-01-01T01:00:00.000000",
                                             "2022-01-02T01:00:00.000000",
                                             "2022-01-03T01:00:00.000000",
                                             "2022-01-04T01:00:00.000000"};
    // Create a Series object with the test vector
    Series s(test_vec);
    // Convert the Series to a DateTimeLike object
    auto dt = s.dt();

    // Test day() function
    REQUIRE(dt.day().values<int64_t>() == std::vector<int64_t>{1, 2, 3, 4});

    // Test day_of_week() function
    REQUIRE(dt.day_of_week().values<int64_t>() == std::vector<int64_t>{5, 6, 0, 1});

    // Test day_of_year() function
    REQUIRE(dt.day_of_year().values<int64_t>() == std::vector<int64_t>{1, 2, 3, 4});

    // Test hour() function
    REQUIRE(dt.hour().values<int64_t>() == std::vector<int64_t>{1, 1, 1, 1});

    // Test is_dst() function
    REQUIRE_THROWS(dt.is_dst().equals(std::vector<bool>{false, false, false, false}));

    // Test iso_week() function
    REQUIRE(dt.iso_week().values<int64_t>() == std::vector<int64_t>{52, 52, 1, 1});

    // Test iso_year() function
    REQUIRE(dt.iso_year().values<int64_t>() == std::vector<int64_t>{2021, 2021, 2022, 2022});

    // Test iso_calendar() function
    auto iso_calendar_result = dt.iso_calendar();

    REQUIRE(iso_calendar_result.at(0, 0) == 2021L);
    REQUIRE(iso_calendar_result.at(0, 1) == 52L);
    REQUIRE(iso_calendar_result.at(0, 2) == 6L);

    REQUIRE(iso_calendar_result.at(1, 0) == 2021L);
    REQUIRE(iso_calendar_result.at(1, 1) == 52L);
    REQUIRE(iso_calendar_result.at(1, 2) == 7L);

    REQUIRE(iso_calendar_result.at(2, 0) == 2022L);
    REQUIRE(iso_calendar_result.at(2, 1) == 1L);
    REQUIRE(iso_calendar_result.at(2, 2) == 1L);

    REQUIRE(iso_calendar_result.at(3, 0) == 2022L);
    REQUIRE(iso_calendar_result.at(3, 1) == 1L);
    REQUIRE(iso_calendar_result.at(3, 2) == 2L);

    // Test is_dst() function
    //    REQUIRE(dt.is_dst().values<int32_t>() == std::vector<int32_t>{0, 0, 0, 0});

    // Test is_leap_year() function
    REQUIRE(dt.is_leap_year().equals(std::vector<bool>{false, false, false, false}));

    // Test microsecond() function
    REQUIRE(dt.microsecond().values<int64_t>() == std::vector<int64_t>{0, 0, 0, 0});

    // Test milliseconds() function
    REQUIRE(dt.millisecond().values<int64_t>() == std::vector<int64_t>{0, 0, 0, 0});

    // Test minute() function
    REQUIRE(dt.minute().values<int64_t>() == std::vector<int64_t>{0, 0, 0, 0});

    // Test month() function
    REQUIRE(dt.month().values<int64_t>() == std::vector<int64_t>{1, 1, 1, 1});

    // Test nanosecond() function
    REQUIRE(dt.nanosecond().values<int64_t>() == std::vector<int64_t>{0, 0, 0, 0});

    // Test quarter() function
    REQUIRE(dt.quarter().values<int64_t>() == std::vector<int64_t>{1, 1, 1, 1});

    // Test second() function
    REQUIRE(dt.second().values<int64_t>() == std::vector<int64_t>{0, 0, 0, 0});

    // Test subsecond() function
    REQUIRE(dt.subsecond().values<double>() == std::vector<double>{0, 0, 0, 0});

    // Test us_week() function
    REQUIRE(dt.us_week().values<int64_t>() == std::vector<int64_t>{52, 1, 1, 1});

    // Test us_year() function
    REQUIRE(dt.us_year().values<int64_t>() == std::vector<int64_t>{2021, 2022, 2022, 2022});

    // Test year() function
    REQUIRE(dt.year().values<int64_t>() == std::vector<int64_t>{2022, 2022, 2022, 2022});

    //    auto year_month_day_result = dt.year_month_day();
    //    REQUIRE(year_month_day_result.at(0, 0) == 2022);
    //    REQUIRE(year_month_day_result.at(0, 1) == 1);
    //    REQUIRE(year_month_day_result.at(0, 2) == 1);
}

// TEST_CASE("Test DateTimeLike between functions", "[datetime]")
//{
//    // Create two Series objects with datetime values
//    Series s1 = Series(std::vector<std::string>{ "2022-01-01 00:00:00",
//                                                 "2022-01-02 00:00:00",
//                                                 "2022-01-03 00:00:00",
//                                                 "2022-01-04 00:00:00" });
//    Series s2 = Series(std::vector<std::string>{ "2022-01-01 12:00:00",
//                                                 "2022-01-02 12:00:00",
//                                                 "2022-01-03 12:00:00",
//                                                 "2022-01-04 12:00:00" }).dt();
//
//    // Test day_time_interval_between() function
//    auto day_time_interval_between_result =
//        s1.dt().day_time_interval_between(s2);
//
//    REQUIRE(
//        day_time_interval_between_result.equals(
//        std::vector<std::string>{ "12:00:00",
//                                  "12:00:00",
//                                  "12:00:00",
//                                  "12:00:00" }));
//
//    // Test days_between() function
//    auto days_between_result = s1.dt().days_between(s2);
//    REQUIRE(
//        days_between_result.values<int64_t_t>() ==
//        std::vector<int64_t_t>{ 0, 0, 0, 0 });
//
//    // Test hours_between() function
//    auto hours_between_result = s1.dt().hours_between(s2);
//    REQUIRE(
//        hours_between_result.values<int64_t_t>() ==
//        std::vector<int64_t_t>{ 12, 12, 12, 12 });
//
//    // Test microseconds_between() function
//    auto microseconds_between_result = s1.dt().microseconds_between(s2);
//    REQUIRE(
//        microseconds_between_result.values<int64_t_t>() ==
//        std::vector<int64_t_t>{ 4320000000000,
//                              4320000000000,
//                              4320000000000,
//                              4320000000000 });
//
//    // Test milliseconds_between() function
//    auto milliseconds_between_result = s1.dt().milliseconds_between(s2);
//    REQUIRE(
//        milliseconds_between_result.values<int64_t_t>() ==
//        std::vector<int64_t_t>{ 4320000000, 4320000000, 4320000000, 4320000000 });
//
//    // Test minutes_between() function
//    auto minutes_between_result = s1.dt().minutes_between(s2);
//    REQUIRE(
//        minutes_between_result.values<int64_t_t>() ==
//        std::vector<int64_t_t>{ 720, 720, 720, 720 });
//
//    // Test month_day_nano_interval_between() function
//    auto month_day_nano_interval_between_result =
//        s1.dt().month_day_nano_interval_between(s2);
//    REQUIRE(
//        month_day_nano_interval_between_result.equals(
//        std::vector<std::string>{ "1 days 00:00:00.000000000",
//                                  "2 days 00:00:00.000000000",
//                                  "3 days 00:00:00.000000000",
//                                  "4 days 00:00:00.000000000" }));
//    // Test month_interval_between() function
//    auto month_interval_between_result = s1.dt().month_interval_between(s2);
//    REQUIRE(
//        month_interval_between_result.values<int64_t>() ==
//        std::vector<int64_t>{ 0, 1, 2, 3 });
//    // Test nanoseconds_between() function
//    auto nanoseconds_between_result = s1.dt().nanoseconds_between(s2);
//    REQUIRE(
//        nanoseconds_between_result.values<int64_t>() ==
//        std::vector<int64_t>{ 0,
//                            86400000000000,
//                            172800000000000,
//                            259200000000000 });
//    // Test quarters_between() function
//    auto quarters_between_result = s1.dt().quarters_between(s2);
//    REQUIRE(
//        quarters_between_result.values<int64_t>() ==
//        std::vector<int64_t>{ 0, 0, 0, 0 });
//    // Test seconds_between() function
//    auto seconds_between_result = s1.dt().seconds_between(s2);
//    REQUIRE(
//        seconds_between_result.values<int64_t>() ==
//        std::vector<int64_t>{ 0, 86400, 172800, 259200 });
//    // Test weeks_between() function
//    auto weeks_between_result = s1.dt().weeks_between(s2);
//    REQUIRE(
//        weeks_between_result.values<int64_t>() ==
//        std::vector<int64_t>{ 0, 0, 0, 0 });
//    // Test years_between() function
//    auto years_between_result = s1.dt().years_between(s2);
//    REQUIRE(
//        years_between_result.values<int64_t>() ==
//        std::vector<int64_t>{ 0, 0, 0, 0 });
//}