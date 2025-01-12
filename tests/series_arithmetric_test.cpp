//
// Created by adesola on 1/11/25.
//
#include <catch.hpp>
#include "pandas_arrow.h"


using namespace std::string_literals;
using namespace Catch;
using namespace pd;

TEST_CASE("Test Series Math Functions", "[series]")
{
    // Test 1: Test basic mathematical operations on Series of ints
    std::vector<long> int_vec = { 1, 2, 3, 4, 5 };
    pd::Series int_series(int_vec);
    REQUIRE((int_series + 2).at(0).as<long>() == 3);
    REQUIRE((int_series - 2).at(0).as<long>() == -1);
    REQUIRE((int_series * 2).at(0).as<long>() == 2);
    REQUIRE((int_series / 2).at(0).as<long>() == 0);
    REQUIRE((-int_series).at(0).as<long>() == -1);

    // Test 2: Test basic mathematical operations on Series of doubles
    std::vector<double> double_vec = { 1.1, 2.2, 3.3, 4.4, 5.5 };
    pd::Series double_series(double_vec);
    REQUIRE((double_series + 2).at(0).as<double>() == Approx(3.1));
    REQUIRE((double_series - 2).at(0).as<double>() == Approx(-0.9));
    REQUIRE((double_series * 2).at(0).as<double>() == Approx(2.2));
    REQUIRE((double_series / 2).at(0).as<double>() == Approx(0.55));
    REQUIRE((-double_series).at(0).as<double>() == Approx(-1.1));

    // Test 3: Test mathematical operations between Series of different types
    std::vector<long> int_vec2 = { 2, 4, 6, 8, 10 };
    pd::Series int_series2(int_vec2);
    REQUIRE((int_series + int_series2).at(0).as<long>() == 3);
    REQUIRE((int_series - int_series2).at(0).as<long>() == -1);
    REQUIRE((int_series * int_series2).at(0).as<long>() == 2);
    REQUIRE((int_series / int_series2).at(0).as<long>() == 0);
    REQUIRE((double_series + int_series2).at(0).as<double>() == Approx(3.1));
    REQUIRE((double_series - int_series2).at(0).as<double>() == Approx(-0.9));
    REQUIRE((double_series - int_series2).at(1).as<double>() == Approx(-1.8));
    REQUIRE((double_series - int_series2).at(2).as<double>() == Approx(-2.7));
    REQUIRE((double_series - int_series2).at(3).as<double>() == Approx(-3.6));
    REQUIRE((double_series - int_series2).at(4).as<double>() == Approx(-4.5));

    SECTION("Test Series Math Functions")
    {
        // Test 1: Addition
        pd::Series int_series2(int_vec, "int_series2");
        pd::Series add_series = int_series + int_series2;
        REQUIRE(add_series.size() == 5);
        REQUIRE(add_series.dtype() == arrow::int64());
        REQUIRE(add_series.name() == "");
        REQUIRE(add_series.at(0).as<long>() == 2);
        REQUIRE(add_series.at(1).as<long>() == 4);
        REQUIRE(add_series.at(2).as<long>() == 6);
        REQUIRE(add_series.at(3).as<long>() == 8);
        REQUIRE(add_series.at(4).as<long>() == 10);

        // Test 2: Subtraction
        pd::Series diff_series = int_series - int_series2;
        REQUIRE(diff_series.size() == 5);
        REQUIRE(diff_series.dtype() == arrow::int64());
        REQUIRE(diff_series.name() == "");
        REQUIRE(diff_series.at(0).as<long>() == 0);
        REQUIRE(diff_series.at(1).as<long>() == 0);
        REQUIRE(diff_series.at(2).as<long>() == 0);
        REQUIRE(diff_series.at(3).as<long>() == 0);
        REQUIRE(diff_series.at(4).as<long>() == 0);

        // Test 3: Subtraction with different types
        pd::Series diff_series2 = double_series - int_series2;
        REQUIRE(diff_series2.size() == 5);
        REQUIRE(diff_series2.dtype() == arrow::float64());
        REQUIRE(diff_series2.name() == "");
        REQUIRE(diff_series2.at(0).as<double>() == Approx(0.1));
        REQUIRE(diff_series2.at(1).as<double>() == Approx(0.2));
        REQUIRE(diff_series2.at(2).as<double>() == Approx(0.3));
        REQUIRE(diff_series2.at(3).as<double>() == Approx(0.4));
        REQUIRE(diff_series2.at(4).as<double>() == Approx(0.5));

        // Test 4: Multiplication
        pd::Series mul_series = int_series * int_series2;
        REQUIRE(mul_series.size() == 5);
        REQUIRE(mul_series.dtype() == arrow::int64());
        REQUIRE(mul_series.name() == "");
        REQUIRE(mul_series.at(0).as<long>() == 1);
        REQUIRE(mul_series.at(1).as<long>() == 4);
        REQUIRE(mul_series.at(2).as<long>() == 9);
        REQUIRE(mul_series.at(3).as<long>() == 16);
        REQUIRE(mul_series.at(4).as<long>() == 25);

        SECTION("Integer Division")
        {
            pd::Series div_series = int_series.cast<double>() / int_series2;
            REQUIRE(div_series.size() == 5);
            REQUIRE(div_series.dtype() == arrow::float64());
            REQUIRE(div_series.name() == "");
            REQUIRE(div_series.at(0).as<double>() == Approx(1.0));
            REQUIRE(div_series.at(1).as<double>() == Approx(1.0));
            REQUIRE(div_series.at(2).as<double>() == Approx(1.0));
            REQUIRE(div_series.at(3).as<double>() == Approx(1.0));
            REQUIRE(div_series.at(4).as<double>() == Approx(1.0));
        }

        SECTION("Float Division")
        {
            pd::Series div_series = int_series / int_series2;
            REQUIRE(div_series.size() == 5);
            REQUIRE(div_series.dtype() == arrow::int64());
            REQUIRE(div_series.name() == "");
            REQUIRE(div_series.at(0).as<long>() == (1));
            REQUIRE(div_series.at(1).as<long>() == (1));
            REQUIRE(div_series.at(2).as<long>() == (1));
            REQUIRE(div_series.at(3).as<long>() == (1));
            REQUIRE(div_series.at(4).as<long>() == (1));
        }
    }
}