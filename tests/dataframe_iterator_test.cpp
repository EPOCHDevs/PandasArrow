//
// Created by adesola on 1/11/25.
//
#include "pandas_arrow.h"
#include "catch.hpp"


using namespace std::string_literals;


TEST_CASE("Test apply method with Series input", "[GroupBy]")
{
    auto df =
            pd::DataFrame(std::map<std::string, std::vector<::int32_t>>{ { "a", { 1, 1, 3, 1, 1, 1, 3, 8, 2, 2 } },
                                                                         { "b", { 10, 9, 8, 7, 6, 5, 4, 3, 2, 1 } } });

    auto groupby = df.group_by("a"s);
    REQUIRE(groupby.groupSize() == 4);

    auto summation = [](pd::Series const& s) -> std::shared_ptr<arrow::Scalar> { return s.sum().scalar; };

    // Apply function to each group
    pd::DataFrame result{ nullptr };

    SECTION("Group synchronously")
    {
        // Apply function to each group
        result = pd::ReturnOrThrowOnFailure(groupby.apply(summation));
    }

// TODO: FIX
//    SECTION("Group ASynchronously")
//    {
//        // Apply function to each group
//        result = pd::ReturnOrThrowOnFailure(groupby.apply_async(summation));
//    }

    // Check that the result DataFrame has the correct number of rows and columns
    REQUIRE(result.num_rows() == groupby.groupSize());
    REQUIRE(result.num_columns() == 2);

    // Check that the values in the result DataFrame are as expected
    REQUIRE(result["a"].values<::int64_t>() == std::vector<int64_t>{ 5, 6, 8, 4 });
    REQUIRE(result["b"].values<int64_t>() == std::vector<int64_t>{ 37, 12, 3, 3 });
}

TEST_CASE("Test apply method with DataFrame input", "[GroupBy]")
{
    auto df =
            pd::DataFrame(std::map<std::string, std::vector<::int32_t>>{ { "a", { 1, 1, 3, 1, 1, 1, 3, 8, 2, 2 } },
                                                                         { "b", { 10, 9, 8, 7, 6, 5, 4, 3, 2, 1 } } });

    auto groupby = df.group_by("a"s);
    REQUIRE(groupby.groupSize() == 4);

    auto summation = [](pd::DataFrame const& s) -> std::shared_ptr<arrow::Scalar> { return s.sum().scalar; };

    pd::Series result{ std::vector<::int64_t>{} };

    SECTION("Group synchronously")
    {
        // Apply function to each group
        result = pd::ReturnOrThrowOnFailure(groupby.apply(summation));
    }

    SECTION("Group ASynchronously")
    {
        // Apply function to each group
        result = pd::ReturnOrThrowOnFailure(groupby.apply_async(summation));
    }

    // Check that the result DataFrame has the correct number of rows and columns
    REQUIRE(result.size() == groupby.groupSize());

    // Check that the values in the result DataFrame are as expected
    REQUIRE(result.values<::int64_t>() == std::vector<int64_t>{ 42, 18, 11, 7 });
}