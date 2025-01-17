//
// Created by dewe on 1/9/23.
//

#include "pandas_arrow.h"
#include "catch.hpp"

using namespace std::string_literals;
using std::pair;
using std::vector;

using namespace pd;

TEST_CASE("Test toTimestampNS with valid input", "[toTimestampNS]")
{
    std::string date_string = "2022-01-01T00:00:00";
    int64_t expected_timestamp = 1640995200000000000; // in ns
    REQUIRE(toTimestampNS(date_string) == expected_timestamp);
}

TEST_CASE("Test fromDateTime function", "[fromDateTime]")
{
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

TEST_CASE("PromoteTypes: empty input", "[promoteTypes]")
{
    std::vector<std::shared_ptr<arrow::DataType>> empty_vec;
    REQUIRE(promoteTypes(empty_vec) == arrow::null());
}

TEST_CASE("PromoteTypes: int32 and int64", "[promoteTypes]")
{
    std::vector<std::shared_ptr<arrow::DataType>> types = { arrow::int32(), arrow::int64() };
    REQUIRE(promoteTypes(types)->id() == arrow::Type::INT64);
}

TEST_CASE("PromoteTypes: int32 and float", "[promoteTypes]")
{
    std::vector<std::shared_ptr<arrow::DataType>> types = { arrow::int32(), arrow::float32() };
    REQUIRE(promoteTypes(types)->id() == arrow::Type::FLOAT);
}

TEST_CASE("PromoteTypes: int32, int64 and float", "[promoteTypes]")
{
    std::vector<std::shared_ptr<arrow::DataType>> types = { arrow::int32(), arrow::int64(), arrow::float64() };
    REQUIRE(promoteTypes(types)->id() == arrow::Type::DOUBLE);
}

TEST_CASE("PromoteTypes: int32 and string", "[promoteTypes]")
{
    std::vector<std::shared_ptr<arrow::DataType>> types = { arrow::int32(), arrow::utf8() };
    REQUIRE(promoteTypes(types) == arrow::utf8());
}

TEST_CASE("PromoteTypes: timestamp and int32", "[promoteTypes]")
{
    std::vector<std::shared_ptr<arrow::DataType>> types = { arrow::timestamp(arrow::TimeUnit::SECOND), arrow::int32() };
    REQUIRE(promoteTypes(types)->id() == arrow::Type::TIMESTAMP);
}

TEST_CASE("Test GetIndexValues") {
    std::vector<int64_t> v{ 1, 2, 3, 4, 5 };

    auto series = pd::Series{v, "vals" };
    REQUIRE(series.getIndexValues<uint64_t>() == std::vector<uint64_t>{ 0, 1, 2, 3, 4});
    REQUIRE_THROWS(series.getIndexValues<int64_t>());

    std::vector<std::string> indexV{"jane", "hello", "hi"};
    series = pd::Series{ std::vector<std::string>{ "1", "2", "3" }, "vals", arrow::ArrayT<std::string>::Make(indexV) };
    REQUIRE(series.getIndexValues<std::string>() == indexV);

    std::vector<bool> indexB{true, false};
    series = pd::Series{ std::vector<std::string>{ "1", "2"}, "vals", arrow::ArrayT<bool>::Make(indexB) };
    REQUIRE(series.getIndexValues<bool>() == indexB);
}