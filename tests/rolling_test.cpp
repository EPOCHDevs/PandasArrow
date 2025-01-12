//
// Created by adesola on 1/11/25.
//
#include "pandas_arrow.h"
#include "catch.hpp"


TEST_CASE("Rolling Invalid")
{
    pd::Series data(std::vector<double>{1, 1, 2, 2, 3, 3});

    std::function fn{[](pd::Series const &x) {
        return x.sum().as<double>();
    }};

    auto result = data.rolling<double>(fn, 10);

    INFO(result);
    REQUIRE(result.equals_(pd::Series(std::vector<double>{})));
}


TEST_CASE("Rolling Series")
{
    pd::Series data(std::vector<double>{1, 1, 2, 2, 3, 3});

    auto sum = [](pd::Series const &x) {
        return x.sum().as<double>();
    };

    auto result = data.rolling<double>(sum, 3);

    INFO(result);
    REQUIRE(result.equals_(pd::Series(
            std::vector<double>{4, 5, 7, 8},
            "",
            data.indexArray()->Slice(2)
    )));

    result = data.expandRolling<double>(sum, 3);

    REQUIRE(result.equals_(pd::Series(
            std::vector<double>{4, 6, 9, 12},
            "",
            data.indexArray()->Slice(2)
    )));
}

TEST_CASE("Rolling Dataframe")
{
    pd::DataFrame data(std::vector<std::vector<double>>{{1, 1, 2, 2, 3, 3},
                                                        {1, 1, 2, 2, 3, 3}},
                       std::vector<std::string>{"x", "y"});
    auto result = data.rolling<double>([](pd::DataFrame const &x) -> double {
        return x.sum().as<double>();
    }, 3);

    INFO(result);
    REQUIRE(result.equals_(pd::Series(
            std::vector<double>{8, 10, 14, 16},
            "",
            data.indexArray()->Slice(2)
    )));
}
