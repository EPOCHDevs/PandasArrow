//
// Created by dewe on 1/9/23.
//

#include "../pandas_arrow.h"
#include "catch.hpp"

using namespace std::string_literals;
using std::pair;
using std::vector;

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

TEST_CASE("Dataframe Concat")
{
    using namespace std::string_literals;
    pd::DataFrame df1{ ArrayPtr{ nullptr },
                       pair{ "letter"s, std::vector<std::string>{ "a", "b"} },
                       pair{ "number"s, std::vector{ 1L, 2L } } };

    pd::DataFrame df2{ ArrayPtr{ nullptr },
                       pair{ "letter"s, std::vector<std::string>{ "c", "d" } },
                       pair{ "number"s, std::vector{ 3L, 4L } } };

    pd::DataFrame df3{ ArrayPtr{ nullptr },
                       pair{ "letter"s, std::vector<std::string>{ "c", "d" } },
                       pair{ "number"s, std::vector{ 3L, 4L } },
                       pair{ "animal"s, std::vector{ "cat"s, "dog"s } } };

    pd::DataFrame df4{ ArrayPtr{ nullptr },
                       pair{ "animal"s, std::vector{ "bird"s, "polly"s } },
                       pair{ "name"s, std::vector{ "monkey"s, "george"s } } };

    DataFrame expected_result{ nullptr }, result{ nullptr };

    SECTION(
        "Combine DataFrame objects with overlapping columns and "
        "return everything. Columns outside the intersection "
        "will be filled with NaN values.",
        "[resample]")
    {

        expected_result = {
            arrow::ArrayFromJSON<::uint64_t>("[0,1,0,1]"),
            pair{ "letter"s, std::vector<std::string>{ "a", "b", "c", "d" } },
            pair{ "number"s, std::vector<long>{ 1L, 2L, 3L, 4L } },
            pair{ "animal"s, std::vector{ ""s, ""s, "cat"s, "dog"s } }
        };

        result = pd::concat(std::vector{ df1, df3 }, AxisType::Index);
        // Assert that the resulting DataFrame is equal to the expected result
        INFO(result << "\n != \n" << expected_result);
        REQUIRE(result.equals_(expected_result));
    }

    SECTION(
        "Combine DataFrame objects horizontally along the x axis",
        "[resample]")
    {
        expected_result = pd::DataFrame{
            arrow::ArrayFromJSON<::uint64_t>("[0,1]"),
            pair{ "letter"s, std::vector<std::string>{ "a", "b" } },
            pair{ "number"s, std::vector<long>{ 1L, 2L } },
            pair{ "animal"s, std::vector{ "bird"s, "polly"s } },
            pair{ "name"s, std::vector{ "monkey"s, "george"s } }
        };

        result = pd::concat(std::vector{ df1, df4 }, AxisType::Columns);
        // Assert that the resulting DataFrame is equal to the expected result
        INFO(result << "\n != \n" << expected_result);
        REQUIRE(result.equals_(expected_result));
    }

    SECTION(
        "Combine DataFrame objects with overlapping columns and return"
        " only those that are shared by passing "
        "inner to the join keyword argument.",
        "[resample]")
    {
        expected_result = pd::DataFrame{
            arrow::ArrayFromJSON<::uint64_t>("[0,1,0,1]"),
            pair{ "letter"s, std::vector<std::string>{ "a", "b", "c", "d" } },
            pair{ "number"s, std::vector{ 1L, 2L, 3L, 4L } }
        };

        result = pd::concat(std::vector{ df1, df3 }, AxisType::Index, JoinType::Inner);
        // Assert that the resulting DataFrame is equal to the expected result
        INFO(result << "\n != \n" << expected_result);
        REQUIRE(result.equals_(expected_result));
    }

    SECTION(
        "Combine two DataFrame objects with identical columns",
        "[resample]")
    {
        expected_result = pd::DataFrame{
            arrow::ArrayFromJSON<::uint64_t>("[0,1,0,1]"),
            pair{ "letter"s, std::vector<std::string>{ "a", "b", "c", "d" } },
            pair{ "number"s, std::vector{ 1L, 2L, 3L, 4L } }
        };

        result = pd::concat(std::vector{ df1, df2 }, AxisType::Index);
        // Assert that the resulting DataFrame is equal to the expected result
        INFO(result << "\n != \n" << expected_result);
        REQUIRE(result.equals_(expected_result));
    }

    SECTION(
        "Combine two DataFrame objects with identical columns, IGNORE_INDEX",
        "[resample]")
    {
        auto d7 = pd::DataFrame{ arrow::ArrayFromJSON<::uint64_t>("[0]"),
                                 std::pair{ "a"s, std::vector{ 1L } },
                                 std::pair{ "b"s, std::vector{ 2L } } };

        auto d8 = pd::DataFrame{ arrow::ArrayFromJSON<::uint64_t>("[0]"),
                                 std::pair{ "a"s, std::vector{ 3L } },
                                 std::pair{ "b"s, std::vector{ 4L } } };

        expected_result =
            pd::DataFrame{ arrow::ArrayFromJSON<::uint64_t>("[0,1]"),
                           pair{ "a"s, std::vector{ 1L, 3L } },
                           pair{ "b"s, std::vector{ 2L, 4L } } };

        result = pd::concat(
            std::vector{ d7, d8 },
            AxisType::Index,
            pd::JoinType::Outer,
            true);

        // Assert that the resulting DataFrame is equal to the expected result
        INFO(result << "\n != \n" << expected_result);
        REQUIRE(result.equals_(expected_result));
    }

}

//TEST_CASE("concatenating DataFrame with Series", "[concat]")
//{
//
//    pd::DataFrame df1{ pd::ArrayTable{
//                           { "s1", arrow::ArrayFromJSON<int>("[1,2,3]") },
//                           { "e", arrow::ArrayFromJSON<int>("[4,5,6]") } },
//                       arrow::ArrayFromJSON<int>("[2,1,4]") };
//
//    pd::Series s1{ arrow::ArrayFromJSON<int>("[1,2,3]"), arrow::ArrayFromJSON<int>("[0,1,2]"), "s1" };
//    pd::Series s2{ arrow::ArrayFromJSON<int>("[4,5,6]"), arrow::ArrayFromJSON<int>("[3,4,5]"), "s2" };
//    pd::Series s3{ arrow::ArrayFromJSON<int>("[7,8,9]"), arrow::ArrayFromJSON<int>("[6,7,8]") };
//
//    std::vector<NDFrameType> objs { s1, s2, df1, s3};
//    pd::DataFrame result = pd::concat(objs, AxisType::Columns);
//
//    pd::DataFrame expected_result{ pd::ArrayTable{
//        { "s1", arrow::ArrayFromJSON<int>("[1, 2, 3, null, null, null]") },
//        { "s2", arrow::ArrayFromJSON<int>("[null, null, null, 4, 5, 6]") },
//        { "s1", arrow::ArrayFromJSON<int>("[null, 2, 1, null, 3, null]") },
//        { "e", arrow::ArrayFromJSON<int>("[null, 5, 4, null, 6, null]") },
//        { "0", arrow::ArrayFromJSON<int>("[null, null, null, null, null, 7, 8, 9]") }
//    }};
//
//    INFO(result << "\n != \n" << expected_result);
//    REQUIRE(result.equals_(expected_result));
//
//}

TEST_CASE("PromoteTypes: empty input", "[promoteTypes]") {
    std::vector<std::shared_ptr<arrow::DataType>> empty_vec;
    REQUIRE(promoteTypes(empty_vec) == arrow::null());
}

TEST_CASE("PromoteTypes: int32 and int64", "[promoteTypes]") {
    std::vector<std::shared_ptr<arrow::DataType>> types = {arrow::int32(), arrow::int64()};
    REQUIRE(promoteTypes(types)->id() == arrow::Type::INT64);
}

TEST_CASE("PromoteTypes: int32 and float", "[promoteTypes]") {
    std::vector<std::shared_ptr<arrow::DataType>> types = {arrow::int32(), arrow::float32()};
    REQUIRE(promoteTypes(types)->id() == arrow::Type::FLOAT);
}

TEST_CASE("PromoteTypes: int32, int64 and float", "[promoteTypes]") {
    std::vector<std::shared_ptr<arrow::DataType>> types = {arrow::int32(), arrow::int64(), arrow::float64()};
    REQUIRE(promoteTypes(types)->id() == arrow::Type::DOUBLE);
}

TEST_CASE("PromoteTypes: int32 and string", "[promoteTypes]") {
    std::vector<std::shared_ptr<arrow::DataType>> types = {arrow::int32(), arrow::utf8()};
    REQUIRE(promoteTypes(types) == arrow::utf8());
}

TEST_CASE("PromoteTypes: timestamp and int32", "[promoteTypes]") {
    std::vector<std::shared_ptr<arrow::DataType>> types = {arrow::timestamp(arrow::TimeUnit::SECOND), arrow::int32()};
    REQUIRE(promoteTypes(types)->id() == arrow::Type::TIMESTAMP);
}

TEST_CASE("ConcatenateRows", "[concatenateRows]")
{

    auto df1 = pd::DataFrame(
        pd::NULL_INDEX,
        pair{ "a"s, vector{ 1L, 2L, 3L } },
        pair{ "b"s, vector{ 4L, 5L, 6L } });

    SECTION("Concatenate two DataFrames with no duplicate keys")
    {
        auto df2 = pd::DataFrame(
            pd::NULL_INDEX,
            pair{ "c"s, vector{ 7L, 8L, 9L } },
            pair{ "d"s, vector{ 10L, 11L, 12L } });

        auto concatenator = pd::Concatenator({ df1, df2 });

        auto result = concatenator(AxisType::Index);
        auto expected = pd::DataFrame(
            ArrayTable{
                { "a",
                  arrow::ArrayFromJSON<int64_t>("[1,2,3,null,null,null]") },
                { "b",
                  arrow::ArrayFromJSON<int64_t>("[4,5,6,null,null,null]") },
                { "c",
                  arrow::ArrayFromJSON<int64_t>("[null,null,null,7,8,9]") },
                { "d",
                  arrow::ArrayFromJSON<int64_t>(
                      "[null,null,null,10,11,12]") } },
            arrow::ArrayFromJSON<::uint64_t>("[0,1,2,0,1,2]"));

        INFO(result << "\n!=\n" << expected);
        REQUIRE(result.equals_(expected));
    }

    SECTION("Concatenate two DataFrames with duplicate keys")
    {
        auto df2 = pd::DataFrame(
            pd::NULL_INDEX,
            pair{ "a"s, vector{ 7L, 8L, 9L } },
            pair{ "b"s, vector{ 10L, 11L, 12L } });

        auto concatenator = pd::Concatenator({ df1, df2 });

        auto result = concatenator(AxisType::Index);
        auto expected = pd::DataFrame(
            TableWithType<int64_t>{ { "a", { 1, 2, 3, 7, 8, 9 } },
                                    { "b", { 4, 5, 6, 10, 11, 12 } } },
            arrow::ArrayFromJSON<::uint64_t>("[0,1,2,0,1,2]"));
        REQUIRE(result.equals_(expected));
    }

    SECTION("Concatenate two DataFrames with different types of duplicate keys")
    {
        auto df2 = pd::DataFrame(
            pd::NULL_INDEX,
            pair{ "a"s, vector<double>{ 7.0, 8.0, 9.0 } },
            pair{ "b"s, vector{ 10L, 11L, 12L } });

        auto concatenator = pd::Concatenator({ df1, df2 });

        auto result = concatenator(AxisType::Index);
        auto expected = pd::DataFrame(
            arrow::ArrayFromJSON<::uint64_t>("[0,1,2,0,1,2]"),
            pair{ "a"s, vector<double>{ 1, 2, 3, 7.0, 8.0, 9.0 } },
            pair{ "b"s, vector<::int64_t>{ 4, 5, 6, 10, 11, 12 } });
        REQUIRE(result.equals_(expected));
    }

    SECTION(
        "Concatenate two DataFrames with duplicate keys and ignore_index = true")
    {
        auto df2 = pd::DataFrame(
            pd::NULL_INDEX,
            pair{ "a"s, vector{ 7L, 8L, 9L } },
            pair{ "b"s, vector{ 10L, 11L, 12L } });

        auto concatenator =
            pd::Concatenator({ df1, df2 }, JoinType::Outer, true);

        auto result = concatenator(AxisType::Index);
        auto expected = pd::DataFrame(
            TableWithType<int64_t>{ { "a", { 1, 2, 3, 7, 8, 9 } },
                                    { "b", { 4, 5, 6, 10, 11, 12 } } });
        REQUIRE(result.equals_(expected));
    }

    SECTION("Concatenate two DataFrames with duplicate keys and sort = true")
    {
        auto df2 = pd::DataFrame(
            pd::NULL_INDEX,
            pair{ "a"s, vector{ 7L, 8L, 9L } },
            pair{ "b"s, vector{ 10L, 11L, 12L } });

        auto concatenator =
            pd::Concatenator({ df1, df2 }, JoinType::Outer, false, true);

        auto result = concatenator(AxisType::Index);
        auto expected = pd::DataFrame(
            ArrayTable{
                { "a", arrow::ArrayFromJSON<int64_t>("[1,2,3,7,8,9]") },
                { "b", arrow::ArrayFromJSON<int64_t>("[4,5,6,10,11,12]") } },
            arrow::ArrayFromJSON<::uint64_t>("[0,1,2,0,1,2]"));

        INFO(result << "\n!=\n" << expected);
        REQUIRE(result.equals_(expected));
    }

    SECTION("Concatenate DataFrames with JoinType::Inner")
    {
        auto df2 = pd::DataFrame(
            pd::NULL_INDEX,
            pair{ "a"s, vector{ 7L, 8L, 9L } },
            pair{ "b"s, vector{ 10L, 11L, 12L } });
        auto concatenator = pd::Concatenator({ df1, df2 }, JoinType::Inner);

        auto result = concatenator(AxisType::Index);
        auto expected = pd::DataFrame(
            ArrayTable{
                { "a", arrow::ArrayFromJSON<int64_t>("[1,2,3,7,8,9]") },
                { "b", arrow::ArrayFromJSON<int64_t>("[4,5,6,10,11,12]") },
            },
            arrow::ArrayFromJSON<::uint64_t>("[0,1,2,0,1,2]"));

        INFO(result << "\n!=\n" << expected);
        REQUIRE(result.equals_(expected));
    }

    SECTION("Concatenate DataFrames with different indexes and JoinType::Inner")
    {
        auto df2 = pd::DataFrame(
            pd::NULL_INDEX,
            pair{ "a"s, vector{ 7L, 8L, 9L } },
            pair{ "c"s, vector{ 10L, 11L, 12L } });
        auto concatenator = pd::Concatenator({ df1, df2 }, JoinType::Inner);

        auto result = concatenator(AxisType::Index);
        auto expected = pd::DataFrame(
            ArrayTable{
                { "a", arrow::ArrayFromJSON<int64_t>("[1,2,3,7,8,9]") },
            },
            arrow::ArrayFromJSON<::uint64_t>("[0,1,2,0,1,2]"));

        INFO(result << "\n!=\n" << expected);
        REQUIRE(result.equals_(expected));
    }

    SECTION("Concatenate two DataFrames with some duplicate keys and sort = true")
    {
        auto df2 = pd::DataFrame(
            pd::NULL_INDEX,
            pair{ "a"s, vector{ 7L, 8L, 9L } },
            pair{ "b"s, vector{ 10L, 11L, 12L } },
            pair{ "e"s, vector{ 13L, 14L, 15L } });
        auto concatenator = pd::Concatenator({ df1, df2 }, JoinType::Outer, false, true);

        auto result = concatenator(AxisType::Index);
        auto expected = pd::DataFrame(
            ArrayTable{
                { "a",
                  arrow::ArrayFromJSON<int64_t>("[1,2,3,7,8,9]") },
                { "b",
                  arrow::ArrayFromJSON<int64_t>("[4,5,6,10,11,12]") },
                { "e",
                  arrow::ArrayFromJSON<int64_t>("[null,null,null,13,14,15]") } },
            arrow::ArrayFromJSON<::uint64_t>("[0,1,2,0,1,2]"));

        INFO(result << "\n!=\n" << expected);
        REQUIRE(result.equals_(expected));
    }

    SECTION("Concatenate two DataFrames with some duplicate keys and sort = false")
    {
        auto df2 = pd::DataFrame(
            pd::NULL_INDEX,
            pair{ "a"s, vector{ 7L, 8L, 9L } },
            pair{ "b"s, vector{ 10L, 11L, 12L } },
            pair{ "e"s, vector{ 13L, 14L, 15L } });

        auto concatenator =
            pd::Concatenator({ df1, df2 }, JoinType::Outer, false, false);

        auto result = concatenator(AxisType::Index);
        auto expected = pd::DataFrame(
            ArrayTable{
                { "a", arrow::ArrayFromJSON<int64_t>("[1,2,3,7,8,9]") },
                { "b", arrow::ArrayFromJSON<int64_t>("[4,5,6,10,11,12]") },
                { "e",
                  arrow::ArrayFromJSON<int64_t>(
                      "[null,null,null,13,14,15]") } },
            arrow::ArrayFromJSON<::uint64_t>("[0,1,2,0,1,2]"));

        INFO(result << "\n!=\n" << expected);
        REQUIRE(result.equals_(expected));
    }
}

TEST_CASE("ConcatenateColumns", "[concatenateColumns]")
{
    auto df1 = pd::DataFrame(
        pd::NULL_INDEX,
        pair{ "a"s, vector{ 1L, 2L, 3L } },
        pair{ "b"s, vector{ 4L, 5L, 6L } });

    SECTION("Concatenate DataFrames with no duplicate keys")
    {
        auto df1 = pd::DataFrame(
            pd::NULL_INDEX,
            pair{ "a"s, vector{ 1L, 2L, 3L } },
            pair{ "b"s, vector{ 4L, 5L, 6L } });

        auto df2 = pd::DataFrame(
            pd::NULL_INDEX,
            pair{ "c"s, vector{ 7L, 8L, 9L } },
            pair{ "d"s, vector{ 10L, 11L, 12L } });

        auto result = pd::concat(std::vector{ df1, df2 }, AxisType::Columns);
        auto expected = pd::DataFrame(
            ArrayTable{ { "a", arrow::ArrayFromJSON<int64_t>("[1,2,3]") },
                        { "b", arrow::ArrayFromJSON<int64_t>("[4,5,6]") },
                        { "c", arrow::ArrayFromJSON<int64_t>("[7,8,9]") },
                        { "d", arrow::ArrayFromJSON<int64_t>("[10,11,12]") } },
            arrow::ArrayFromJSON<::uint64_t>("[0,1,2]"));

        REQUIRE(result.equals_(expected));
    }

    SECTION("Concatenate two DataFrames with duplicate keys")
    {
        auto df1 = pd::DataFrame(
            pd::NULL_INDEX,
            pair{ "a"s, vector{ 1L, 2L, 3L } },
            pair{ "b"s, vector{ 4L, 5L, 6L } });
        auto df2 = pd::DataFrame(
            pd::NULL_INDEX,
            pair{ "a"s, vector{ 7L, 8L, 9L } },
            pair{ "b"s, vector{ 10L, 11L, 12L } });

        auto result = pd::concat(
            std::vector<pd::DataFrame>{ df1, df2 },
            AxisType::Columns);
        auto expected = pd::DataFrame(
            std::vector<std::vector<long>>{ { { 1L, 2L, 3L },
                                              { 4L, 5L, 6L },
                                              { 7, 8, 9 },
                                              { 10, 11, 12 } } },
            { "a", "b", "a", "b" });

        INFO(result << "\n!=\n" << expected);
        REQUIRE(result.equals_(expected));
    }

    SECTION("Concatenate two DataFrames with different types of duplicate keys")
    {
        auto df1 = pd::DataFrame(
            pd::NULL_INDEX,
            pair{ "a"s, vector{ 1L, 2L, 3L } },
            pair{ "b"s, vector{ 4L, 5L, 6L } });
        auto df2 = pd::DataFrame(
            pd::NULL_INDEX,
            pair{ "a"s, vector<double>{ 7.0, 8.0, 9.0 } },
            pair{ "b"s, vector{ 10L, 11L, 12L } });

        auto result = pd::concat(
            std::vector<pd::DataFrame>{ df1, df2 },
            AxisType::Columns);
        auto expected = pd::DataFrame(
            std::array{ "a"s, "b"s, "a"s, "b"s },
            NULL_INDEX,
            std::vector{ 1L, 2L, 3L },
            std::vector{ 4L, 5L, 6L },
            std::vector{ 7.0, 8.0, 9.0 },
            std::vector{ 10L, 11L, 12L });

        INFO(result << "\n!=\n" << expected);
        REQUIRE(result.equals_(expected));
    }

    SECTION(
        "Concatenate two DataFrames with duplicate keys and ignore_index = true")
    {
        auto df1 = pd::DataFrame(
            pd::NULL_INDEX,
            pair{ "a"s, vector{ 1L, 2L, 3L } },
            pair{ "b"s, vector{ 4L, 5L, 6L } });
        auto df2 = pd::DataFrame(
            pd::NULL_INDEX,
            pair{ "a"s, vector{ 7L, 8L, 9L } },
            pair{ "b"s, vector{ 10L, 11L, 12L } });

        auto result = pd::concat(
            std::vector<pd::DataFrame>{ df1, df2 },
            AxisType::Columns,
            JoinType::Outer,
            true);
        auto expected = pd::DataFrame(
            std::vector<std::vector<long>>{ { { 1L, 2L, 3L },
                                              { 4L, 5L, 6L },
                                              { 7, 8, 9 },
                                              { 10, 11, 12 } } },
            { "0", "1", "2", "3" });

        INFO(result << "\n!=\n" << expected);
        REQUIRE(result.equals_(expected));
    }

    SECTION("Concatenate two DataFrames with some duplicate keys and sort = true")
    {
        auto df1 = pd::DataFrame(
            pd::NULL_INDEX,
            pair{ "d"s, vector{ 1L, 2L, 3L } },
            pair{ "e"s, vector{ 4L, 5L, 6L } });

        auto df2 = pd::DataFrame(
            pd::NULL_INDEX,
            pair{ "c"s, vector{ 7L, 8L, 9L } },
            pair{ "e"s, vector{ 10L, 11L, 12L } });

        auto result = pd::concat(
            std::vector<pd::DataFrame>{ df1, df2 },
            AxisType::Columns,
            JoinType::Outer,
            false,
            true);

        auto expected = pd::DataFrame(
            std::array<std::string, 4>{ "d", "e", "c", "e" },
            pd::ArrayPtr{ nullptr },
            std::vector{ 1L, 2L, 3L},
            std::vector{ 4L, 5L, 6L },
            std::vector{ 7L, 8L, 9L },
            std::vector{ 10L, 11L, 12L });

        INFO(result << "\n!=\n" << expected);
        REQUIRE(result.equals_(expected));
    }

    SECTION("Concatenate two DataFrames with some duplicate keys and sort = false")
    {
        auto df1 = pd::DataFrame(
            pd::NULL_INDEX,
            pair{ "d"s, vector{ 1L, 2L, 3L } },
            pair{ "e"s, vector{ 4L, 5L, 6L } });

        auto df2 = pd::DataFrame(
            pd::NULL_INDEX,
            pair{ "c"s, vector{ 7L, 8L, 9L } },
            pair{ "e"s, vector{ 10L, 11L, 12L } });

        auto result = pd::concat(
            std::vector<pd::DataFrame>{ df1, df2 },
            AxisType::Columns,
            JoinType::Outer,
            false,
            false);

        auto expected = pd::DataFrame(
            std::array<std::string, 4>{ "d", "e", "c", "e" },
            pd::ArrayPtr{ nullptr },
            std::vector{ 1L, 2L, 3L},
            std::vector{ 4L, 5L, 6L },
            std::vector{ 7L, 8L, 9L },
            std::vector{ 10L, 11L, 12L });

        INFO(result << "\n!=\n" << expected);
        REQUIRE(result.equals_(expected));
    }

    SECTION("Concatenate DataFrames with different indexes")
    {
        auto df1 = pd::DataFrame(
            pd::NULL_INDEX,
            pair{ "a"s, vector{ 1L, 2L, 3L } },
            pair{ "b"s, vector{ 4L, 5L, 6L } });

        auto df2 = pd::DataFrame(
            pd::NULL_INDEX,
            pair{ "a"s, vector{ 7L, 8L, 9L } },
            pair{ "c"s, vector{ 10L, 11L, 12L } });

        auto result = pd::concat(
            std::vector<pd::DataFrame>{ df1, df2 },
            AxisType::Columns);

        auto expected = pd::DataFrame(
            std::array<std::string, 4>{ "a", "b", "a", "c" },
            pd::ArrayPtr{ nullptr },
            std::vector{ 1L, 2L, 3L },
            std::vector{ 4L, 5L, 6L },
            std::vector{ 7L, 8L, 9L },
            std::vector{ 10L, 11L, 12L });

        INFO(result << "\n!=\n" << expected);
        REQUIRE(result.equals_(expected));
    }

    SECTION("Concatenate DataFrames with different indexes and ignore_index = true")
    {

        auto df2 = pd::DataFrame(
            pd::NULL_INDEX,
            pair{ "a"s, vector{ 7L, 8L, 9L } },
            pair{ "c"s, vector{ 10L, 11L, 12L } });

        auto result = pd::concat(
            std::vector<pd::DataFrame>{ df1, df2 },
            AxisType::Columns,
            JoinType::Outer,
            true);

        auto expected = pd::DataFrame(
            std::array<std::string, 4>{ "0", "1", "2", "3" },
            pd::ArrayPtr{ nullptr },
            std::vector{ 1L, 2L, 3L },
            std::vector{ 4L, 5L, 6L },
            std::vector{ 7L, 8L, 9L },
            std::vector{ 10L, 11L, 12L });

        INFO(result << "\n!=\n" << expected);
        REQUIRE(result.equals_(expected));
    }

    SECTION("Concatenate DataFrames with JoinType::Inner")
    {
        auto df2 = pd::DataFrame(
            pd::NULL_INDEX,
            pair{ "a"s, vector{ 7L, 8L, 9L } },
            pair{ "b"s, vector{ 10L, 11L, 12L } });
        auto result = pd::concat(
            std::vector<pd::DataFrame>{ df1, df2 },
            AxisType::Index,
            JoinType::Inner);

            auto expected = pd::DataFrame(
                ArrayTable{
                    { "a", arrow::ArrayFromJSON<int64_t>("[1,2,3,7,8,9]") },
                    { "b", arrow::ArrayFromJSON<int64_t>("[4,5,6,10,11,12]") },
                },
                arrow::ArrayFromJSON<::uint64_t>("[0,1,2,0,1,2]"));

        INFO(result << "\n!=\n" << expected);
        REQUIRE(result.equals_(expected));
    }
    SECTION("Concatenate DataFrames with different indexes and JoinType::Inner")
    {
        auto df2 = pd::DataFrame(
            pd::NULL_INDEX,
            pair{ "a"s, vector{ 7L, 8L, 9L } },
            pair{ "c"s, vector{ 10L, 11L, 12L } });

        auto result = pd::concat(
            std::vector<pd::DataFrame>{ df1, df2 },
            AxisType::Index,
            JoinType::Inner);

            auto expected = pd::DataFrame(
                ArrayTable{
                    { "a", arrow::ArrayFromJSON<int64_t>("[1,2,3,7,8,9]") },
                },
                arrow::ArrayFromJSON<::uint64_t>("[0,1,2,0,1,2]"));

        INFO(result << "\n!=\n" << expected);
        REQUIRE(result.equals_(expected));
    }
}

TEST_CASE("Concatenate with Series", "[concat]")
{
    SECTION(
        "Concatenating multiple Series with different indexes,"
        " axis = 0, join = outer, ignore_index = false, sort = false")
    {
        auto s1 =
            pd::Series(std::vector{ 1L, 2L, 3L }, "s1", pd::range(0UL, 3UL));
        auto s2 =
            pd::Series(std::vector{ 4L, 5L, 6L }, "", pd::range(3UL, 6UL));
        auto s3 =
            pd::Series(std::vector{ 7L, 8L, 9L }, "", pd::range(6UL, 9UL));
        auto result = pd::concat(
            std::vector<pd::Series>{ s1, s2, s3 },
            AxisType::Index,
            JoinType::Outer,
            false,
            false);
        auto expected = pd::DataFrame(
            std::array<std::string, 1>{ "" },
            pd::ArrayPtr{ nullptr },
            std::vector<long>{ 1, 2, 3, 4, 5, 6, 7, 8, 9 },
            std::vector<::uint64_t>{ 0, 1, 2, 3, 4, 5, 6, 7, 8 });
        INFO(result << "\n!=\n" << expected);
        REQUIRE(result.equals_(expected));
    }

    SECTION(
        "Concatenating multiple Series with different indexes, axis = 1, join = outer, ignore_index = true, sort = true")
    {
        auto s1 =
            pd::Series(std::vector{ 1L, 2L, 3L }, "s1", pd::range(0UL, 3UL));
        auto s2 =
            pd::Series(std::vector{ 4L, 5L, 6L }, "s2", pd::range(3UL, 6UL));
        auto s3 =
            pd::Series(std::vector{ 7L, 8L, 9L }, "s3", pd::range(6UL, 9UL));
        auto result = pd::concat(
            std::vector<pd::Series>{ s1, s2, s3 },
            AxisType::Columns,
            JoinType::Outer,
            true,
            true);
        auto expected = pd::DataFrame(
            ArrayTable{ { "s1",
                          arrow::ArrayFromJSON<::int64_t>(
                              "[1,2,3,null,null,null,null,null,null]") },
                        { "s2",
                          arrow::ArrayFromJSON<::int64_t>(
                              "[null,null,null,4,5,6,null,null,null]") },
                        { "s3",
                          arrow::ArrayFromJSON<::int64_t>(
                              "[null,null,null,null,null,null,7,8,9]") } });
        INFO(result << "\n!=\n" << expected);
        REQUIRE(result.equals_(expected));
    }

    SECTION(
        "Concatenating multiple Series with different indexes, axis = 1, join = outer, ignore_index = true, sort = true")
    {
        auto s1 =
            pd::Series(std::vector{ 1L, 2L, 3L }, "s1", pd::range(0UL, 3UL));
        auto s2 =
            pd::Series(std::vector{ 4L, 5L, 6L }, "s2", pd::range(3UL, 6UL));
        auto s3 =
            pd::Series(std::vector{ 7L, 8L, 9L }, "s3", pd::range(6UL, 9UL));
        auto result = pd::concat(
            std::vector<pd::Series>{ s1, s2, s3 },
            AxisType::Columns,
            JoinType::Outer,
            true,
            true);
        auto expected = pd::DataFrame(
            ArrayTable{ { "s1",
                          arrow::ArrayFromJSON<::int64_t>(
                              "[1,2,3,null,null,null,null,null,null]") },
                        { "s2",
                          arrow::ArrayFromJSON<::int64_t>(
                              "[null,null,null,4,5,6,null,null,null]") },
                        { "s3",
                          arrow::ArrayFromJSON<::int64_t>(
                              "[null,null,null,null,null,null,7,8,9]") } });
        INFO(result << "\n!=\n" << expected);
        REQUIRE(result.equals_(expected));
    }

    SECTION(
        "Concatenating multiple Series with duplicate keys,"
        " axis = 0, join = inner, ignore_index = false, sort = false")
    {
        auto s1 =
            pd::Series(std::vector{ 1L, 2L, 3L }, "a", pd::range(0UL, 3UL));
        auto s2 =
            pd::Series(std::vector{ 4L, 5L, 6L }, "b", pd::range(3UL, 6UL));
        auto s3 =
            pd::Series(std::vector{ 7L, 8L, 9L }, "a", pd::range(6UL, 9UL));
        auto result = pd::concat(
            std::vector<pd::Series>{ s1, s2, s3 },
            AxisType::Index,
            JoinType::Inner,
            false,
            false);
        auto expected = pd::DataFrame(
            std::array<std::string, 1>{ "" },
            pd::ArrayPtr{ nullptr },
            std::vector<long>{ 1, 2, 3, 4, 5, 6, 7, 8, 9 },
            std::vector<::uint64_t>{ 0, 1, 2, 6, 7, 8 });
        INFO(result << "\n!=\n" << expected);
        REQUIRE(result.equals_(expected));
    }

    SECTION(
        "Concatenating multiple Series with duplicate keys,"
        " axis = 0, join = inner, ignore_index = false, sort = false")
    {
        auto s1 =
            pd::Series(std::vector{ 1L, 2L, 3L }, "a", pd::range(0UL, 3UL));
        auto s2 =
            pd::Series(std::vector{ 4L, 5L, 6L }, "b", pd::range(3UL, 6UL));
        auto s3 =
            pd::Series(std::vector{ 7L, 8L, 9L }, "a", pd::range(6UL, 9UL));
        auto result = pd::concat(
            std::vector<pd::Series>{ s1, s2, s3 },
            AxisType::Index,
            JoinType::Inner,
            false,
            false);
        auto expected = pd::DataFrame(
            std::array<std::string, 1>{ "" },
            pd::ArrayPtr{ nullptr },
            std::vector<long>{ 1, 2, 3, 4, 5, 6, 7, 8, 9 });
        INFO(result << "\n!=\n" << expected);
        REQUIRE(result.equals_(expected));
    }
    SECTION(
        "Concatenating multiple Series with duplicate keys,"
        " axis = 0, join = inner, ignore_index = false, sort = true")
    {
        auto s1 =
            pd::Series(std::vector{ 1L, 2L, 3L }, "a", pd::range(0UL, 3UL));
        auto s2 =
            pd::Series(std::vector{ 4L, 5L, 6L }, "b", pd::range(3UL, 6UL));
        auto s3 =
            pd::Series(std::vector{ 7L, 8L, 9L }, "a", pd::range(6UL, 9UL));
        auto result = pd::concat(
            std::vector<pd::Series>{ s1, s2, s3 },
            AxisType::Index,
            JoinType::Inner,
            false,
            true);
        auto expected = pd::DataFrame(
            std::array<std::string, 1>{ "a" },
            pd::ArrayPtr{ nullptr },
            std::vector<long>{ 1, 2, 3, 4, 5, 6, 7, 8, 9 });
        INFO(result << "\n!=\n" << expected);
        REQUIRE(result.equals_(expected));
    }

//    SECTION("Concatenating a mix of DataFrames and Series with different indexes and types,"
//        " axis = 1, join = inner, ignore_index = false, sort = false") {
//        auto s1 = pd::Series(std::vector{1L, 2L, 3L}, "s1", pd::range(0UL, 3UL));
//        auto df1 = pd::DataFrame(
//            pd::NULL_INDEX,
//            pair{ "a"s, vector{ 4L, 5L, 6L } },
//            pair{ "b"s, vector{ 7L, 8L, 9L } });
//        auto s2 = pd::Series(std::vector{10L, 11L, 12L}, "s2", pd::range(3UL, 6UL));
//        auto df2 = pd::DataFrame(
//            pd::NULL_INDEX,
//            pair{ "c"s, vector{ 13L, 14L, 15L } },
//            pair{ "d"s, vector{ 16L, 17L, 18L } });
//        auto result = pd::concat(
//            std::vector<NDFrameType>{s1, df1, s2, df2},
//            AxisType::Columns,
//            JoinType::Inner,
//            false,
//            false);
//        auto expected = pd::DataFrame(
//            std::array<std::string, 4>{ "s1", "a", "b", "s2" },
//            pd::ArrayPtr{ nullptr },
//            std::vector{ 1, 2, 3 },
//            std::vector{ 4, 5, 6 },
//            std::vector{ 7, 8, 9 },
//            std::vector{ 10, 11, 12 }
//        );
//        INFO(result << "\n!=\n" << expected);
//        REQUIRE(result.equals_(expected));
//    }
}

//TEST_CASE("Concatenate Benchmark", "[concat]")
//{
//    auto rand = pd::random::RandomState(100);
//    auto generateTable = [&rand](bool int_type, int N, ArrayTable& data)
//    {
//        for (int i = 0; i < N; i++)
//        {
//            data[std::to_string(rand.randint(1, 0, 50)[0])] = int_type ?
//                (ArrayPtr)arrow::ArrayT<::int64_t>::Make(
//                    rand.randint(1e6, 0, 100)) :
//                (ArrayPtr)arrow::ArrayT<double>::Make(rand.rand(1e6));
//        }
//    };
//
//    ArrayTable df1, df2, df3, df4, df5;
//
//    generateTable(true, 5, df1);
//    generateTable(false, 5, df1);
//    generateTable(true, 10, df1);
//
//    generateTable(true, 5, df2);
//    generateTable(true, 5, df2);
//    generateTable(true, 10, df2);
//
//    generateTable(false, 5, df3);
//    generateTable(false, 5, df3);
//    generateTable(false, 10, df3);
//
//    generateTable(true, 5, df4);
//    generateTable(false, 5, df4);
//    generateTable(false, 10, df4);
//
//    generateTable(false, 5, df5);
//    generateTable(false, 5, df5);
//    generateTable(true, 10, df5);
//
////    {
////        auto start = std::chrono::high_resolution_clock::now();
////
////        auto result = pd::concat(std::vector{ pd::DataFrame(df1),
////                                              pd::DataFrame(df2),
////                                              pd::DataFrame(df3),
////                                              pd::DataFrame(df4),
////                                              pd::DataFrame(df5) });
////
////        auto stop = std::chrono::high_resolution_clock::now();
////        auto duration = std::chrono::duration<double>(stop - start);
////
////        std::cout << "Index concat: took " << duration.count() << " seconds\nshape = ";
////        std::cout << result.shape()[0] << " " <<  result.shape()[1] << "]\n";
////    }
//    {
//        auto start = std::chrono::high_resolution_clock::now();
//
//        auto result = pd::concat(std::vector{ pd::DataFrame(df1, pd::range(0l, 1e6l)),
//                                              pd::DataFrame(df2, pd::range(10l, 1e6l+10l)),
//                                              pd::DataFrame(df3, pd::range(0l, 1e6l)),
//                                              pd::DataFrame(df4, pd::range(5l, 1e6l+5l)),
//                                              pd::DataFrame(df5, pd::range(0l, 1e6l)) },
//                                 AxisType::Columns);
//
//        auto stop = std::chrono::high_resolution_clock::now();
//        auto duration = std::chrono::duration<double>(stop - start);
//
//        std::cout << "Column concat: took " << duration.count() << " seconds\nshape = ";
//        std::cout << result.shape()[0] << " " <<  result.shape()[1] << "]\n";
//    }
//
////    {
////        auto start = std::chrono::high_resolution_clock::now();
////
////        auto result = pd::concatColumnsUnsafe(std::vector{ pd::DataFrame(df1, pd::range(0l, 1e6l)),
////                                              pd::DataFrame(df2, pd::range(10l, 1e6l+10l)),
////                                              pd::DataFrame(df3, pd::range(0l, 1e6l)),
////                                              pd::DataFrame(df4, pd::range(5l, 1e6l+5l)),
////                                              pd::DataFrame(df5, pd::range(0l, 1e6l)) });
////
////        auto stop = std::chrono::high_resolution_clock::now();
////        auto duration = std::chrono::duration<double>(stop - start);
////
////        std::cout << "Column unsafe concat: took " << duration.count() << " seconds\nshape = ";
////        std::cout << result.shape()[0] << " " <<  result.shape()[1] << "]\n";
////    }
//
//}