//
// Created by dewe on 1/9/23.
//

#include "../pandas_arrow.h"
#include "catch.hpp"

using namespace std::string_literals;


using namespace pd;
TEST_CASE("Test date_range with end date", "[date_range]") {
    auto start = date(2022, 1, 1);
    auto end = date(2022, 1, 7);
    auto freq = "D";
    auto tz = "UTC";

    auto result = pd::date_range(start, end, freq, tz);

    REQUIRE(result != nullptr);
    REQUIRE(result->length() == 7);
    REQUIRE(result->type()->id() == arrow::Type::TIMESTAMP);
}

TEST_CASE("Test date_range with period", "[date_range]") {
    auto start = date(2022, 1, 1);
    auto period = 7;
    auto freq = "D";
    auto tz = "UTC";

    auto result = pd::date_range(start, period, freq, tz);

    REQUIRE(result != nullptr);
    REQUIRE(result->length() == 7);
    REQUIRE(result->type()->id() == arrow::Type::TIMESTAMP);

    start = date(2022, 1, 1);
    period = 10;

    result = date_range(start, period, "D");
    REQUIRE(result->length() == 10);

    result = date_range(start, period, "SM");
    REQUIRE(result->length() == 10);

    result = date_range(start, period, "W");
    REQUIRE(result->length() == 10);
}

TEST_CASE("Test date_range with different frequency", "[date_range]")
{
    date start(2022, 1, 1);
    date end(2022, 1, 10);
    std::shared_ptr<arrow::Array> result;

    result = date_range(start, end, "D");
    REQUIRE(result->length() == 10);

    result = date_range(start, end, "W");
    REQUIRE(result->length() == 2);

    result = date_range(start, end, "SM");
    REQUIRE(result->length() == 1);

    start = date(2022, 1, 1);
    end = date(2022, 1, 31);
    auto freq = "W";
    auto tz = "UTC";

    result = pd::date_range(start, end, freq, tz);

    REQUIRE(result != nullptr);
    REQUIRE(result->length() == 5);
    REQUIRE(result->type()->id() == arrow::Type::TIMESTAMP);
}

TEST_CASE("Test date_range with different timezone", "[date_range]") {
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

    result = date_range(start, end, "D", "UTC");
    REQUIRE(result->length() == 10);

    result = date_range(start, end, "D", "US/Pacific");
    REQUIRE(result->length() == 10);
}

TEST_CASE("Test date_range with invalid frequency", "[date_range]") {
    auto start = date(2022, 1, 1);
    auto end = date(2022, 1, 7);
    auto freq = "X";
    auto tz = "UTC";

    REQUIRE_THROWS(pd::date_range(start, end, freq, tz));
    REQUIRE_THROWS(date_range(start, 5, "InvalidFrequency"));
}

TEST_CASE("Test date_range with invalid timezone", "[date_range]") {
    auto start = date(2022, 1, 1);
    auto end = date(2022, 1, 7);
    auto freq = "D";
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
    int period = 0;
    REQUIRE_THROWS(date_range(start, period, "D"));
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

    std::vector<int64_t> expected_values = {
        1640995200000000000,
        1640995200000000000 + 86400000000000,
        1640995200000000000 + 86400000000000*2,
        1640995200000000000 + 86400000000000*3,
        1640995200000000000 + 86400000000000*4,
        1640995200000000000 + 86400000000000*5,
        1640995200000000000 + 86400000000000*6
    };
    auto timestamp_array = std::static_pointer_cast<arrow::TimestampArray>(result);
    for (int i = 0; i < result->length(); i++) {
        REQUIRE(timestamp_array->Value(i) == expected_values[i]);
    }

    auto result_freq = date_range(start, end, "2D");
    REQUIRE(result_freq->length() == 4);

    expected_values = {
        1640995200000000000,
        1640995200000000000 + 86400000000000*2,
        1640995200000000000 + 86400000000000*4,
        1640995200000000000 + 86400000000000*6
    };

    timestamp_array = std::static_pointer_cast<arrow::TimestampArray>(result_freq);
    for (int i = 0; i < result_freq->length(); i++) {
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
    auto timestamp_array =
        arrow::checked_pointer_cast<arrow::TimestampArray>(result);

    auto start_timestamp = fromDate(start);
    for (int i = 0; i < period; i++)
    {
        REQUIRE(timestamp_array->Value(i) == start_timestamp + i * 86400 * 1e9);
    }
}

TEST_CASE("Test date_range with valid input and frequency", "[date_range]") {

    SECTION("Test frequency of 'D'") {
        date start(2022, 1, 1);
        int period = 10;
        auto result = date_range(start, period, "D");
        REQUIRE(result->length() == 10);
        REQUIRE(result->type()->id() == arrow::Type::TIMESTAMP);

        //Checking that the first timestamp is 2022-01-01
        auto timestamp = arrow::checked_pointer_cast<arrow::TimestampScalar>( result->GetScalar(0).MoveValueUnsafe() );
        date first_date = toDate(timestamp->value);
        REQUIRE(first_date == date(2022, 1, 1));

        //Checking that the last timestamp is 2022-01-10
        timestamp = arrow::checked_pointer_cast<arrow::TimestampScalar>( result->GetScalar(result->length() - 1).MoveValueUnsafe() );
        date last_date = toDate(timestamp->value);
        REQUIRE(last_date == date(2022, 1, 10));
    }

    SECTION("Test frequency of 'W'") {
        date start(2022, 1, 1);
        int period = 2;

        auto result = date_range(start, period, "W");
        REQUIRE(result->length() == 2);
        REQUIRE(result->type()->id() == arrow::Type::TIMESTAMP);

        //Checking that the first timestamp is 2022-01-01
        auto timestamp = arrow::checked_pointer_cast<arrow::TimestampScalar>( result->GetScalar(0).MoveValueUnsafe() );
        date first_date = toDate(timestamp->value);
        REQUIRE(first_date == date(2022, 1, 1));

        //Checking that the last timestamp is 2022-01-08
        timestamp = arrow::checked_pointer_cast<arrow::TimestampScalar>( result->GetScalar(result->length() - 1).MoveValueUnsafe() );
        date last_date = toDate(timestamp->value);
        REQUIRE(last_date == date(2022, 1, 8));
    }

    SECTION("Test frequency of 'M'") {
        date start(2022, 1, 31);
        int period = 1;

        auto result = date_range(start, period, "M");
        REQUIRE(result->length() == 1);
        REQUIRE(result->type()->id() == arrow::Type::TIMESTAMP);

        //Checking that the first timestamp is 2022-01-31
        auto timestamp = arrow::checked_pointer_cast<arrow::TimestampScalar>( result->GetScalar(0).MoveValueUnsafe() );
        date first_date = toDate(timestamp->value);
        REQUIRE(first_date == date(2022, 1, 31));
    }

    SECTION("Test frequency of 'Y'") {
        date start(2022, 12, 31);
        int period = 1;

        auto result = date_range(start, period, "Y");
        REQUIRE(result->length() == 1);
        REQUIRE(result->type()->id() == arrow::Type::TIMESTAMP);

        //Checking that the first timestamp is 2022-12-31
        auto timestamp = arrow::checked_pointer_cast<arrow::TimestampScalar>( result->GetScalar(0).MoveValueUnsafe() );
        date first_date = toDate(timestamp->value);
        REQUIRE(first_date == date(2022, 12, 31));
    }
}

TEST_CASE("Test firstDateOfYear, lastDateOfYear, and firstDateOfMonth", "[date_functions]") {
    date d1(2022, 2, 15);
    date d2(2022, 12, 31);
    date d3(2022, 12, 1);

    // Test firstDateOfYear
    REQUIRE(firstDateOfYear(d1) == date(2022, 1, 1));
    REQUIRE(firstDateOfYear(d2) == date(2022, 1, 1));

    // Test lastDateOfYear
    REQUIRE(lastDateOfYear(d1) == date(2022, 12, 31));
    REQUIRE(lastDateOfYear(d2) == date(2022, 12, 31));

    // Test firstDateOfMonth
    REQUIRE(firstDateOfMonth(d1) == date(2022, 2, 1));
    REQUIRE(firstDateOfMonth(d3) == date(2022, 12, 1));
}

TEST_CASE("Test toTimestampNS with valid input", "[toTimestampNS]") {
    std::string date_string = "2022-01-01T00:00:00";
    int64_t expected_timestamp = 1640995200000000000; // in ns
    REQUIRE(toTimestampNS(date_string) == expected_timestamp);
}

TEST_CASE("Test fromDateTime function", "[fromDateTime]") {
    date test_date(2022, 1, 1);
    ptime test_ptime(test_date);
    auto date_result = fromDateTime(test_date);
    auto ptime_result = fromDateTime(test_ptime);

    REQUIRE(date_result->type->id() == arrow::Type::TIMESTAMP);
    REQUIRE(ptime_result->type->id() == arrow::Type::TIMESTAMP);

    auto date_timestamp = arrow::checked_pointer_cast<arrow::TimestampScalar>(date_result);
    auto ptime_timestamp = arrow::checked_pointer_cast<arrow::TimestampScalar>(ptime_result);

    auto date_ns = date_timestamp->value;
    auto ptime_ns = ptime_timestamp->value;

    REQUIRE(date_ns == ptime_ns);
}

TEST_CASE("Test resample with valid input", "[resample]") {
    // Create a test DataFrame with a datetime index
    auto df = pd::DataFrame(
        date_range(ptime(date(2000, 1, 1)), 9),
        std::pair("value"s, std::vector{0, 1, 2, 3, 4, 5, 6, 7, 8}));

    auto resampled = pd::resample(df, boost::posix_time::minutes(3), true, true);

    auto sum = ValidateAndReturn( resampled.mean("resampled") );

    REQUIRE(sum.size() == 3);
}