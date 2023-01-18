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