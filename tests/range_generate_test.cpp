//
// Created by adesola on 1/11/25.
//
#include "pandas_arrow.h"
#include "catch.hpp"

using namespace pd;

TEST_CASE("Test date_range with start and end date", "[date_range]")
{
    auto start = date(2018, Jan, 1);
    auto end = date(2018, Jan, 8);
    auto freq = "1D";
    auto tz = "UTC";

    auto result = arrow::checked_pointer_cast<arrow::TimestampArray>(pd::date_range(start, end, freq, tz));

    REQUIRE(result != nullptr);
    REQUIRE(result->length() == 8);

    for (int i = 0; i < 8; ++i)
    {
        REQUIRE(toTimeNanoSecPtime(result->Value(i)).date() == start);
        start += days(1);
    }
}

TEST_CASE("Test date_range with period", "[date_range]") {
    auto start = date(2018, Jan, 1);
    auto period = 8;


    auto result = arrow::checked_pointer_cast<arrow::TimestampArray>(pd::date_range(start, period, "1D"));
    REQUIRE(result->length() == period);

    auto x = start;
    for (int i = 0; i < 8; ++i) {
        REQUIRE(toTimeNanoSecPtime(result->Value(i)).date() == x);
        x += days(1);
    }

    REQUIRE(result != nullptr);
    REQUIRE(result->length() == 8);

    start = date(2022, 1, 1);
    period = 10;

    result = date_range(start, period, "1D");
    REQUIRE(result->length() == 10);

    result = date_range(start, period, "1MS");
    REQUIRE(result->length() == 10);

    result = date_range(start, period, "1WS");
    REQUIRE(result->length() == 10);
}

TEST_CASE("Test date_range with different frequency", "[date_range]")
{
    date start(2022, 1, 1);
    date end(2022, 1, 10);
    std::shared_ptr<arrow::Array> result;

    result = date_range(start, end, "1D");
    REQUIRE(result->length() == 10);

    result = date_range(start, end, "1WS");
    REQUIRE(result->length() == 2);

    result = date_range(start, end, "1MS");
    REQUIRE(result->length() == 1);

    start = date(2022, 1, 1);
    end = date(2022, 1, 31);
    auto freq = "WS";
    auto tz = "UTC";

    result = pd::date_range(start, end, freq, tz);

    REQUIRE(result != nullptr);
    REQUIRE(result->length() == 5);
    REQUIRE(result->type()->id() == arrow::Type::TIMESTAMP);
}

TEST_CASE("Test date_range with different timezone", "[date_range]")
{
    auto start = date(2022, 1, 1);
    auto end = date(2022, 1, 7);
    auto freq = "D";
    auto tz = "CET";

    auto result = pd::date_range(start, end, freq, tz);

    REQUIRE(result != nullptr);
    REQUIRE(result->length() == 7);
    REQUIRE(result->type()->id() == arrow::Type::TIMESTAMP);

    start = date(2022, 1, 1);
    end = date(2022, 1, 10);

    result = date_range(start, end, "1D", "UTC");
    REQUIRE(result->length() == 10);

    result = date_range(start, end, "1D", "US/Pacific");
    REQUIRE(result->length() == 10);
}

TEST_CASE("Test date_range with invalid frequency", "[date_range]")
{
    auto start = date(2022, 1, 1);
    auto end = date(2022, 1, 7);
    auto freq = "X";
    auto tz = "UTC";

    REQUIRE_THROWS(pd::date_range(start, end, freq, tz));
    REQUIRE_NOTHROW(date_range(start, 5, "InvalidFrequency"));
}

TEST_CASE("Test date_range with invalid timezone", "[date_range]")
{
    auto start = date(2022, 1, 1);
    auto end = date(2022, 1, 7);
    auto freq = "1D";
    auto tz = "InvalidTZ";

    REQUIRE_NOTHROW(pd::date_range(start, end, freq, tz));
}

TEST_CASE("Test date_range with invalid date range", "[date_range]")
{
    date start(2022, 1, 1);
    date end(2020, 1, 1);
    REQUIRE_THROWS(pd::date_range(start, end));

    start = date(2022, 1, 1);
    end = date(2022, 1, 2);
    REQUIRE_NOTHROW(pd::date_range(start, end));
}

TEST_CASE("Test date_range with invalid period", "[date_range]")
{
    date start(2022, 1, 1);
    int period = -1;
    REQUIRE_THROWS(date_range(start, period, "D"));
}

TEST_CASE("Test date_range with 0 period", "[date_range]")
{
    date start(2022, 1, 1);
    int period = 0;
    REQUIRE(date_range(start, period, "D")->length() == 0);
}

TEST_CASE("Test date_range with negative period", "[date_range]")
{
    date start(2020, 1, 1);
    int period = -5;
    std::string freq = "D";
    std::string tz = "UTC";

    REQUIRE_THROWS(date_range(start, period, freq, tz));
}

TEST_CASE("Test date_range with valid input", "[date_range]")
{
    auto start = date(2022, 1, 1);
    auto end = date(2022, 1, 7);
    auto result = date_range(start, end);

    REQUIRE(result->length() == 7);

    std::vector<int64_t> expected_values = { 1640995200000000000,
                                             1640995200000000000 + 86400000000000,
                                             1640995200000000000 + 86400000000000 * 2,
                                             1640995200000000000 + 86400000000000 * 3,
                                             1640995200000000000 + 86400000000000 * 4,
                                             1640995200000000000 + 86400000000000 * 5,
                                             1640995200000000000 + 86400000000000 * 6 };
    auto timestamp_array = std::static_pointer_cast<arrow::TimestampArray>(result);
    for (int i = 0; i < result->length(); i++)
    {
        REQUIRE(timestamp_array->Value(i) == expected_values[i]);
    }

    auto result_freq = date_range(start, end, "2D");
    REQUIRE(result_freq->length() == 4);

    expected_values = { 1640995200000000000,
                        1640995200000000000 + 86400000000000 * 2,
                        1640995200000000000 + 86400000000000 * 4,
                        1640995200000000000 + 86400000000000 * 6 };

    timestamp_array = std::static_pointer_cast<arrow::TimestampArray>(result_freq);
    for (int i = 0; i < result_freq->length(); i++)
    {
        REQUIRE(timestamp_array->Value(i) == expected_values[i]);
    }
}

TEST_CASE("Test date_range with valid input and period", "[date_range]")
{
    auto start = date(2022, 1, 1);
    auto period = 10;
    auto result = date_range(start, period, "D");
    REQUIRE(result->length() == 10);
    auto array = result->data();
    auto timestamp_array = arrow::checked_pointer_cast<arrow::TimestampArray>(result);

    auto start_timestamp = fromDate(start);
    for (int i = 0; i < period; i++)
    {
        REQUIRE(timestamp_array->Value(i) == start_timestamp + i * 86400 * 1e9);
    }
}

TEST_CASE("Test date_range with valid input and frequency", "[date_range]")
{

    SECTION("Test frequency of 'D'")
    {
        date start(2022, 1, 1);
        int period = 10;
        auto result = date_range(start, period, "D");
        REQUIRE(result->length() == 10);
        REQUIRE(result->type()->id() == arrow::Type::TIMESTAMP);

        // Checking that the first timestamp is 2022-01-01
        auto timestamp = arrow::checked_pointer_cast<arrow::TimestampScalar>(result->GetScalar(0).MoveValueUnsafe());
        date first_date = toDate(timestamp->value);
        REQUIRE(first_date == date(2022, 1, 1));

        // Checking that the last timestamp is 2022-01-10
        timestamp = arrow::checked_pointer_cast<arrow::TimestampScalar>(
                result->GetScalar(result->length() - 1).MoveValueUnsafe());
        date last_date = toDate(timestamp->value);
        REQUIRE(last_date == date(2022, 1, 10));
    }

    SECTION("Test frequency of 'W'")
    {
        date start(2022, 1, 1);
        int period = 2;

        auto result = date_range(start, period, "WS");
        REQUIRE(result->length() == 2);
        REQUIRE(result->type()->id() == arrow::Type::TIMESTAMP);

        // Checking that the first timestamp is 2022-01-01
        auto timestamp = arrow::checked_pointer_cast<arrow::TimestampScalar>(result->GetScalar(0).MoveValueUnsafe());
        date first_date = toDate(timestamp->value);
        REQUIRE(first_date == date(2022, 1, 1));

        // Checking that the last timestamp is 2022-01-08
        timestamp = arrow::checked_pointer_cast<arrow::TimestampScalar>(
                result->GetScalar(result->length() - 1).MoveValueUnsafe());
        date last_date = toDate(timestamp->value);
        REQUIRE(last_date == date(2022, 1, 8));
    }

    SECTION("Test frequency of 'M'")
    {
        date start(2022, 1, 31);
        int period = 1;

        auto result = date_range(start, period, "MS");
        REQUIRE(result->length() == 1);
        REQUIRE(result->type()->id() == arrow::Type::TIMESTAMP);

        // Checking that the first timestamp is 2022-01-31
        auto timestamp = arrow::checked_pointer_cast<arrow::TimestampScalar>(result->GetScalar(0).MoveValueUnsafe());
        date first_date = toDate(timestamp->value);
        REQUIRE(first_date == date(2022, 1, 31));
    }

    SECTION("Test frequency of 'Y'")
    {
        date start(2022, 12, 31);
        int period = 1;

        auto result = date_range(start, period, "YS");
        REQUIRE(result->length() == 1);
        REQUIRE(result->type()->id() == arrow::Type::TIMESTAMP);

        // Checking that the first timestamp is 2022-12-31
        auto timestamp = arrow::checked_pointer_cast<arrow::TimestampScalar>(result->GetScalar(0).MoveValueUnsafe());
        date first_date = toDate(timestamp->value);
        REQUIRE(first_date == date(2022, 12, 31));
    }
}

// TODO: FIX TEST -> should have date_Range instead
//TEST_CASE("generate_bins", "[Core]")
//{
//    auto value = pd::range(1L, 7L);
//    std::vector<std::tuple<std::vector<::int64_t>, bool, std::vector<::int64_t>>> params{
//            { { 0, 3, 6, 9 }, false, { 2, 5, 6 } },
//            { { 0, 3, 6, 9 }, true, { 3, 6, 6 } },
//            { { 0, 3, 6 }, false, { 2, 5 } },
//            { { 0, 3, 6 }, true, { 3, 6 } }
//    };
//
//    for (auto const& [binner, closed_right, expected] : params)
//    {
//        DYNAMIC_SECTION("binner_size" << binner.size() << " closed_right=" << closed_right)
//        {
//            REQUIRE(pd::generate_bins_dt64(
//                    value, arrow::ArrayT<std::int64_t>::Make(binner), closed_right) == expected);
//        }
//    }
//}