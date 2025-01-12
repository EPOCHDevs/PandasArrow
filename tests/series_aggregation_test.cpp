//
// Created by adesola on 1/11/25.
//
#include <catch.hpp>
#include "pandas_arrow.h"


using namespace std::string_literals;
using namespace Catch;
using namespace pd;

TEST_CASE("Test Series all() method")
{
    std::vector<bool> mask = { false, true, true, false, true };
    Series s1(mask);
    REQUIRE(s1.all() == false);

    std::vector<bool> mask2 = { true, true, true, true, true };
    Series s2(mask2);
    REQUIRE(s2.all() == true);
}

TEST_CASE("Test Series::any() function")
{
    std::vector<int> vec1 = { 1, 2, 3, 4, 5 };
    auto array1 = arrow::ArrayT<int>::Make(vec1);
    Series s1(array1, true);

    // Test with non bool
    REQUIRE_THROWS_AS(s1.any(), std::runtime_error);

    // Test with some null values
    std::vector<bool> mask = { true, false, true, true, false };
    auto array2 = arrow::ArrayT<bool>::Make(mask);
    Series s2(array2, true);
    REQUIRE(s2.any() == true);

    // Test with all null values
    std::vector<bool> mask2 = { false, false, false, false, false };
    auto array3 = arrow::ArrayT<bool>::Make(mask2);
    Series s3(array3, true);
    REQUIRE(s3.any() == false);
}

TEST_CASE("Test Series::count() function")
{
    std::vector<int> vec1 = { 1, 2, 3, 4, 5 };
    auto array1 = arrow::ArrayT<int>::Make(vec1);
    Series s1(array1, true);

    REQUIRE(s1.count() == 5);

    std::vector<int> vec2 = { 1, 2, 3, 4, 5, 6, 7 };
    auto array2 = arrow::ArrayT<int>::Make(vec2);
    Series s2(array2, true);
    REQUIRE(s2.count() == 7);

    std::vector<int> vec3 = { 1, 2, 3, 4, 5, 0, 7 };
    auto array3 = arrow::ArrayT<int>::Make(vec3);
    Series s3(array3, true);
    REQUIRE(s3.count() == 7);
}

TEST_CASE("Test Series::count_na() function")
{
    std::vector<int> vec1 = { 1, 2, 3, 4, 5 };
    auto array1 = arrow::ArrayT<int>::Make(vec1);
    Series s1(array1, true);

    REQUIRE(s1.count_na() == 0);

    std::vector<int> vec2 = { 1, 2, 3, 4, 5, 6, 7 };
    std::vector<bool> mask = { true, true, true, true, true, true, false };
    auto array2 = arrow::ArrayT<int>::Make(vec2, mask);
    Series s2(array2, true);
    REQUIRE(s2.count_na() == 1);

    std::vector<int> vec3 = { 1, 2, 3, 4, 5, 0, 7 };
    std::vector<bool> mask2 = { true, true, true, true, true, false, true };
    auto array3 = arrow::ArrayT<int>::Make(vec3, mask2);
    Series s3(array3, true);

    REQUIRE(s3.count() == 6);
    REQUIRE(s3.count_na() == 1);

    std::vector<int> vec4 = { 1, 2, 3, 4, 5, 0, 7, 1, 2, 3, 4, 5 };
    auto array4 = arrow::ArrayT<int>::Make(vec4);
    Series s4(array4, true);

    REQUIRE(s4.nunique() == 7);

    std::vector<int> vec5 = { 1, 2, 3, 4, 5, 0, 7, 1, 2, 3, 4, 5, -1 };
    mask.clear();
    mask.resize(vec5.size(), true);
    mask.back() = false;

    auto array5 = arrow::ArrayT<int>::Make(vec5, mask);
    Series s5(array5, true);

    REQUIRE(s5.nunique() == 7);
}


TEST_CASE("Test Series unique() methods")
{
    std::vector<int> vec1 = {1, 2, 3, 4, 5, 1, 2, 3, 4, 5, 6};
    auto array1 = arrow::ArrayT<int>::Make(vec1);
    Series s1(array1, true);

    auto result = s1.unique();

    REQUIRE(result.size() == 6);
    REQUIRE(result.at(0) == 1);
    REQUIRE(result.at(1) == 2);
    REQUIRE(result.at(2) == 3);
    REQUIRE(result.at(3) == 4);
    REQUIRE(result.at(4) == 5);
    REQUIRE(result.at(5) == 6);
}


TEST_CASE("Test min_max function for Series", "[series]")
{
    pd::Series s(std::vector<int>{ 1, 2, 3, 4, 5 });
    auto min_max = s.min_max(false);
    REQUIRE(min_max.min.as<int>() == 1);
    REQUIRE(min_max.max.as<int>() == 5);

    s = pd::Series(arrow::ArrayT<int>::Make({ 1, 2, 3, 4, 5 }, { true, true, true, true, false }), nullptr);
    min_max = s.min_max(true);
    REQUIRE(min_max.min == 1);
    REQUIRE(min_max.max == 4);
}

TEST_CASE("Test min function for Series", "[series]")
{
    auto array = arrow::ArrayT<int>::Make({ 1, 2, 3, 4, 5 }, { true, true, true, true, false });
    pd::Series s(array, nullptr);
    auto result = s.min();
    REQUIRE(result.as<int>() == 1);
}

TEST_CASE("Test max function for Series", "[series]")
{
    auto array = arrow::ArrayT<int>::Make({ 1, 2, 3, 4, 5 }, { true, true, true, true, false });
    pd::Series s(array, nullptr);
    auto result = s.max();
    REQUIRE(result.as<int>() == 4);
}

TEST_CASE("Test product function for Series", "[series]")
{
    auto array = arrow::ArrayT<int>::Make({ 1, 2, 3, 4, 5 }, { true, true, true, true, false });
    pd::Series s(array, nullptr);
    auto result = s.product();
    REQUIRE(result.as<long>() == 24);

    array = arrow::ArrayT<int>::Make({ 1, 2, 3, 4, 5 });
    s = pd::Series(array, nullptr);
    result = s.product();
    REQUIRE(result.as<long>() == 120);
}

TEST_CASE("Test median function for Series", "[series]")
{
    pd::Series s(std::vector<int>{ 1, 2, 3, 4, 5 });
    auto result = s.median();
    REQUIRE(result == 3.0);

    s = pd::Series(std::vector<int>{ 1, 2, 3, 4, 5 }, { true, true, true, true, false });
    result = s.median(true);
    REQUIRE(result == 3.0);

    s = pd::Series(std::vector<int>{ 1, 2, 3, 4, 5 }, { false, true, true, true, true });
    result = s.median(true);
    REQUIRE(result == 3.0);

    s = pd::Series(std::vector<int>{ 1, 2, 3, 4, 5, 6 }, { true, true, true, true, true, false });
    result = s.median();
    REQUIRE(result == 3.5);
}

TEST_CASE("Test mean function for Series", "[series]")
{
    pd::Series s(std::vector<int>{ 1, 2, 3, 4, 5 });
    auto result = s.mean();
    REQUIRE(result == 3.0);

    s = pd::Series(arrow::ArrayT<int>::Make({ 1, 2, 3, 4, 5 }, { true, true, true, true, false }), nullptr);
    result = s.mean(true);
    REQUIRE(result == 2.5);

    s = pd::Series(arrow::ArrayT<int>::Make({ 1, 2, 3, 4, 5 }, { false, true, true, true, true }), nullptr);
    result = s.mean(true);
    REQUIRE(result == 3.5);
}

TEST_CASE("Test std function for Series", "[series]")
{
    pd::Series s(std::vector<double>{ 1, 2, 3, 4, 5 });
    auto result = s.std();
    REQUIRE(result.as<double>() == Approx(1.5811388300841898));

    s = pd::Series(arrow::ArrayT<double>::Make({ 1, 2, 3, 4, 5 }, { true, true, true, true, false }), nullptr);
    result = s.std(0, true);
    REQUIRE(result.as<double>() == Approx(1.118033988749895).epsilon(1e-6));

    result = s.std(0, false);
    INFO(result.value()->ToString());
    REQUIRE_FALSE(result.isValid());
}

TEST_CASE("Test var function for Series", "[series]")
{
    pd::Series s(std::vector<double>{ 1, 2, 3, 4, 5 });
    auto result = s.var();
    REQUIRE(result.as<double>() == Approx(2.5));

    s = pd::Series(arrow::ArrayT<double>::Make({ 1, 2, 3, 4, NAN }, { true, true, true, true, false }), nullptr);
    result = s.var(0, true);
    REQUIRE(result.as<double>() == Approx(1.25));

    result = s.var(0, false);
    INFO(result.value()->ToString());
    REQUIRE_FALSE(result.isValid());
}

TEST_CASE("Test mode function for Series", "[series]")
{
    pd::Series s(std::vector<int>{ 1, 2, 3, 4, 5, 2, 2, 2, 3 });
    auto df = s.mode(1, false);

    REQUIRE(df[0].mode == 2);
    REQUIRE(df[0].count == 4L);

    df = s.mode(2, false);

    REQUIRE(df[0].mode == 2);
    REQUIRE(df[0].count == 4L);

    REQUIRE(df[1].mode == 3);
    REQUIRE(df[1].count == 2L);

    s = pd::Series(
            arrow::ArrayT<int>::Make({ 1, 1, 3, 4, 5, 2, 2, 2 }, { true, true, true, true, false, false, false, true }),
            nullptr);
    df = s.mode(1, true);
    REQUIRE(df[0].mode == 1);
    REQUIRE(df[0].count == 2L);

    df = s.mode(1, false);
    REQUIRE(df.size() == 0);
}

TEST_CASE("Test quantile function for Series", "[series]")
{
    pd::Series s(std::vector<int>{ 1, 2, 3, 4, 5 });
    auto result = s.quantile(0.5);
    REQUIRE(result.as<double>() == 3);

    s = pd::Series(std::vector<int>{ 1, 2, 3, 4, 5 }, { true, true, true, true, false });
    result = s.quantile(0.5);
    REQUIRE(result.as<double>() == 3);
}

TEST_CASE("Test tdigest function for Series", "[series]")
{
    pd::Series s(std::vector<int>{ 1, 2, 3, 4, 5 });
    auto result = s.tdigest(0.5);
    REQUIRE(result.as<double>() == 3);

    s = pd::Series(std::vector<int>{ 1, 2, 3, 4, 5 }, { true, true, true, true, false });
    result = s.tdigest(0.5);
    REQUIRE(result.as<double>() == 3);
}

TEST_CASE("Test agg function for Series", "[series]")
{
    ArrayPtr array = arrow::ArrayT<int>::Make({ 1, 2, 3, 4, 5 });
    pd::Series s(array, nullptr);

    auto result = s.agg("sum");
    REQUIRE(result.as<long>() == 15);

    array = arrow::ArrayT<double>::Make({ 1, 2, 3, 4, NAN }, { true, true, true, true, false });
    s = pd::Series(array, nullptr);

    result = s.agg("sum", true);
    REQUIRE(result.as<double>() == 10);

    result = s.agg("sum", false);
    REQUIRE_FALSE(result.isValid());
}

TEST_CASE("Test Series::argmax() and Series::argmin() functions")
{

    SECTION("Test with valid values")
    {
        std::vector<double> vec1 = { 1.0, 2.0, 3.0, 4.0, 5.0 };
        auto array1 = arrow::ArrayT<double>::Make(vec1);
        Series s1(array1, true);

        // Test argmax() function
        REQUIRE(s1.argmax() == 4);

        // Test argmin() function
        REQUIRE(s1.argmin() == 0);
    }

    SECTION("Test with missing values")
    {
        std::vector<double> vec1 = { 1.0, 2.0, 3.0, 4.0, 5.0, std::numeric_limits<double>::quiet_NaN() };
        auto array1 = arrow::ArrayT<double>::Make(vec1, { true, true, true, true, true, false });
        Series s1(array1, true);
        REQUIRE(s1.argmax() == 4);
        REQUIRE(s1.argmin() == 0);
    }
}