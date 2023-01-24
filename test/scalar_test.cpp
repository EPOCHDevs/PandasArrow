//
// Created by dewe on 1/15/23.
//
#include "../scalar.h"
#include <catch.hpp>

TEST_CASE("Scalar construction") {
    // Test default constructor
    pd::Scalar defaultScalar;
    REQUIRE_FALSE(defaultScalar.isValid());

    // Test constructor with shared pointer
    auto arrowScalar = arrow::MakeScalar(5);
    pd::Scalar sharedPtrScalar(arrowScalar);
    REQUIRE(sharedPtrScalar.isValid());
    REQUIRE(sharedPtrScalar.as<int>() == 5);

    // Test move constructor
    pd::Scalar moveScalar(std::move(arrowScalar));
    REQUIRE(moveScalar.isValid());
    REQUIRE(moveScalar.as<int>() == 5);

    // Test copy constructor
    pd::Scalar copyScalar(sharedPtrScalar);
    REQUIRE(copyScalar.isValid());
    REQUIRE(copyScalar.as<int>() == 5);

    // Test constructor with non-string scalar
    pd::Scalar intScalar(5);
    REQUIRE(intScalar.isValid());
    REQUIRE(intScalar.as<int>() == 5);

    // Test constructor with string scalar
    pd::Scalar stringScalar("hello");
    REQUIRE(stringScalar.isValid());
    REQUIRE(stringScalar.as<std::string>() == "hello");
}

TEST_CASE("Scalar as() function") {
    // Test as() function for int
    pd::Scalar intScalar(5);
    REQUIRE(intScalar.as<int>() == 5);

    // Test as() function for double
    pd::Scalar doubleScalar(5.5);
    REQUIRE(doubleScalar.as<double>() == Catch::Approx(5.5));

    // Test as() function for string
    pd::Scalar stringScalar("hello");
    REQUIRE(stringScalar.as<std::string>() == "hello");

    // Test as() function with invalid cast
    pd::Scalar intScalar2(5);
    REQUIRE_THROWS_AS(intScalar2.as<std::string>(), std::runtime_error);
}

TEST_CASE("Scalar MinMax struct") {
    // Test MinMax struct
    pd::MinMax minMax{pd::Scalar(5), pd::Scalar(10)};
    REQUIRE(minMax.min.as<int>() == 5);
    REQUIRE(minMax.max.as<int>() == 10);
}

TEST_CASE("Scalar arithmetic operator overloads", "[scalar]") {

    using namespace pd;
    Scalar a(2);
    Scalar b(3);
    Scalar c(4.5);
    Scalar d(1.5);

    SECTION("Addition") {
        REQUIRE(a + b == Scalar(5));
        REQUIRE(a + 3 == Scalar(5));
        REQUIRE(2 + b == Scalar(5));
        REQUIRE(c + d == Scalar(6.0));
    }

    SECTION("Subtraction") {
        REQUIRE(a - b == Scalar(-1));
        REQUIRE(b - a == Scalar(1));
        REQUIRE(c - d == Scalar(3.0));
        REQUIRE(d - c == Scalar(-3.0));
    }

    SECTION("Multiplication") {
        REQUIRE(a * b == Scalar(6));
        REQUIRE(b * 3 == Scalar(9));
        REQUIRE(2 * b == Scalar(6));
        REQUIRE(c * d == Scalar(6.75));
    }

    SECTION("Division") {
        REQUIRE( (b / a.cast<double>()) == Scalar(1.5) );
        REQUIRE(b / a == Scalar(1));
        REQUIRE(c / d == Scalar(3.0));
        REQUIRE(d / c == Scalar(1 / 3.0));
    }

    SECTION("Comparison") {
        REQUIRE(a < b);
        REQUIRE(a <= b);
        REQUIRE(b > a);
        REQUIRE(b >= a);
        REQUIRE(c == Scalar(4.5));
        REQUIRE(d != Scalar(4.5));
    }

    SECTION("Logical") {
        REQUIRE( (a.cast<bool>() && b.cast<bool>()).as<bool>() == true);
        REQUIRE( (!a.cast<bool>() || b.cast<bool>()).as<bool>() == true);
    }
}
