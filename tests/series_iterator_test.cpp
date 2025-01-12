//
// Created by adesola on 1/11/25.
//
#include <catch.hpp>
#include "pandas_arrow.h"


using namespace std::string_literals;
using namespace Catch;
using namespace pd;

TEST_CASE("Test Series::apply() function")
{
    std::vector<int> vec1 = { 1, 2, 3, 4, 5 };
    auto array1 = arrow::ArrayT<int>::Make(vec1);
    Series s1(array1, true);

    auto squared = [](int x) { return x * x; };

    auto result = s1.apply<int, int>(squared);
    REQUIRE(result.size() == 5);
    REQUIRE(result.at(0) == 1);
    REQUIRE(result.at(1) == 4);
    REQUIRE(result.at(2) == 9);
    REQUIRE(result.at(3) == 16);
    REQUIRE(result.at(4) == 25);

    auto add_and_square = [](int x, int y) { return (x + y) * (x + y); };
    result = s1.apply<int, int>(add_and_square, 2);
    REQUIRE(result.size() == 5);
    REQUIRE(result.at(0) == 9);
    REQUIRE(result.at(1) == 16);
    REQUIRE(result.at(2) == 25);
    REQUIRE(result.at(3) == 36);
    REQUIRE(result.at(4) == 49);
}