//
// Created by adesola on 1/12/25.
//
#include <catch.hpp>
#include "pandas_arrow.h"


using namespace std::string_literals;
using namespace Catch;
using namespace pd;


TEST_CASE("DataFrame::where with DataFrame (multi-column)", "[DataFrame]") {
    // Prepare test data
    DataFrame df1(nullptr, pd::GetRow("a", {1, 2, 3, 4}), pd::GetRow("b", {5, 6, 7, 8}));
    DataFrame cond(nullptr, pd::GetRow("a", std::vector<bool>{true, false, true, false}), pd::GetRow("b", std::vector<bool>{false, true, false, true}));
    DataFrame df2(nullptr, pd::GetRow("a", {10, 20, 30, 40}), pd::GetRow("b", {50, 60, 70, 80}));

    // Expected result:
    // a: [1, 20, 3, 40]
    // b: [50, 6, 70, 8]
    DataFrame expected(nullptr, pd::GetRow("a", {1, 20, 3, 40}), pd::GetRow("b", {50, 6, 70, 8}));

    // Run the method
    DataFrame result = df1.where(cond, df2);

    // Validate
    INFO(result);
    REQUIRE(result.equals_(expected));
}

TEST_CASE("DataFrame::where with Series (multi-column)", "[DataFrame]") {
    // Prepare test data
    DataFrame df1(nullptr, pd::GetRow("a", {1, 2, 3, 4}), pd::GetRow("b", {5, 6, 7, 8}));
    DataFrame cond(nullptr, pd::GetRow("a", std::vector<bool>{true, false, true, false}), pd::GetRow("b", std::vector<bool>{false, true, false, true}));
    Series series = Series(std::vector(4UL, 99));

    // Expected result:
    // a: [1, 99, 3, 99]
    // b: [99, 6, 99, 8]
    DataFrame expected(nullptr, pd::GetRow("a", {1, 99, 3, 99}), pd::GetRow("b", {99, 6, 99, 8}));

    // Run the method
    DataFrame result = df1.where(cond, series);

    // Validate
    INFO(result);
    REQUIRE(result.equals_(expected));
}

TEST_CASE("DataFrame::where with Scalar (multi-column)", "[DataFrame]") {
    // Prepare test data
    DataFrame df1(nullptr, pd::GetRow("a", {1, 2, 3, 4}), pd::GetRow("b", {5, 6, 7, 8}));
    DataFrame cond(nullptr, pd::GetRow("a", std::vector<bool>{true, false, true, false}), pd::GetRow("b",std::vector<bool> {false, true, false, true}));
    Scalar scalar(42);

    // Expected result:
    // a: [1, 42, 3, 42]
    // b: [42, 6, 42, 8]
    DataFrame expected(nullptr, pd::GetRow("a", {1, 42, 3, 42}), pd::GetRow("b", {42, 6, 42, 8}));

    // Run the method
    DataFrame result = df1.where(cond, scalar);

    // Validate
    INFO(result);
    REQUIRE(result.equals_(expected));
}

TEST_CASE("DataFrame::where with NULL (multi-column)", "[DataFrame]") {
    // Prepare test data
    DataFrame df1(nullptr, pd::GetRow("a", {1, 2, 3, 4}), pd::GetRow("b", {5, 6, 7, 8}));
    DataFrame cond(nullptr, pd::GetRow("a", std::vector<bool>{true, false, true, false}),
                   pd::GetRow("b", std::vector<bool>{false, true, false, true}));

    // Run the method
    DataFrame result = df1.where(cond);

    // Validate
    REQUIRE(result.at(0, "a").as<int>() == Approx(1.0));
    REQUIRE_FALSE(result.at(1, "a").isValid());
    REQUIRE(result.at(2, "a").as<int>() == Approx(3.0));
    REQUIRE_FALSE(result.at(3, "a").isValid());

    REQUIRE_FALSE(result.at(0, "b").isValid());
    REQUIRE(result.at(1, "b").as<int>() == Approx(6.0));
    REQUIRE_FALSE(result.at(2, "b").isValid());
    REQUIRE(result.at(3, "b").as<int>() == Approx(8.0));
}