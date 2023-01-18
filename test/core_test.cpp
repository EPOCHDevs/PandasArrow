//
// Created by dewe on 1/9/23.
//

#include "../pandas_arrow.h"
#include "catch.hpp"

using namespace std::string_literals;


TEST_CASE("Test Concat Rows")
{
    pd::DataFrame df(
        pd::range(0, 4),
        std::pair("x"s, std::vector<int>{1, 2, 4, 5}),
        std::pair("y"s, std::vector<int>{2, 2, 4, 5}));

    pd::DataFrame df2(
        pd::range(0, 2),
        std::pair("a"s, std::vector<int>{2, 4}),
        std::pair("y"s, std::vector<int>{4, 5}));

    SECTION("Join outer")
    {
        auto result = pd::concatRows({df, df2});

        std::cout << result << "\n";
    }

    SECTION("Join inner")
    {
        auto result = pd::concatRows({df, df2},
                                        pd::JoinType::Inner);

        std::cout << result << "\n";
    }

    SECTION("Join outer ignore index")
    {
        auto result = pd::concatRows({df, df2},
                                     pd::JoinType::Outer,
                                     true);

        std::cout << result << "\n";
    }

    SECTION("Join inner ignore index")
    {
        auto result = pd::concatRows({df, df2},
                                     pd::JoinType::Inner,
                                     true);

        std::cout << result << "\n";
    }

    SECTION("Join outer sort column")
    {
        auto result = pd::concatRows({df, df2},
                                     pd::JoinType::Outer,
                                     false,
                                     true);

        std::cout << result << "\n";
    }

    SECTION("Join inner sort column")
    {
        auto result = pd::concatRows({df, df2},
                                     pd::JoinType::Inner,
                                     false,
                                     true);

        std::cout << result << "\n";
    }
}


TEST_CASE("Test Concat Columns")
{
    pd::DataFrame df(
        pd::range(0, 4),
        std::pair("x"s, std::vector<int>{ 1, 2, 4, 5 }),
        std::pair("y"s, std::vector<int>{ 2, 2, 4, 5 }));

    pd::DataFrame df2(
        pd::range(0, 2),
        std::pair("a"s, std::vector<int>{ 2, 4 }),
        std::pair("y"s, std::vector<int>{ 4, 5 }));

    SECTION("Join outer")
    {
        auto result = pd::concatColumns({ df, df2 });

        std::cout << result << "\n";
    }

    SECTION("Join inner")
    {
        auto result = pd::concatColumns({ df, df2 }, pd::JoinType::Inner);

        std::cout << result << "\n";
    }

    SECTION("Join outer ignore index")
    {
        auto result = pd::concatColumns({ df, df2 }, pd::JoinType::Outer, true);

        std::cout << result << "\n";
    }

    SECTION("Join inner ignore index")
    {
        auto result = pd::concatColumns({ df, df2 }, pd::JoinType::Inner, true);

        std::cout << result << "\n";
    }

    SECTION("Join outer sort column")
    {
        auto result =
            pd::concatColumns({ df, df2 }, pd::JoinType::Outer, false, true);

        std::cout << result << "\n";
    }

    SECTION("Join inner sort column")
    {
        auto result =
            pd::concatColumns({ df, df2 }, pd::JoinType::Inner, false, true);

        std::cout << result << "\n";
    }
}