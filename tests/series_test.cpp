//
// Created by dewe on 1/15/23.
//
#include <catch.hpp>
#include <rapidjson/document.h>
#include "pandas_arrow.h"
#include "stdexcept"

bool operator==(std::shared_ptr<arrow::DataType> const& a, std::shared_ptr<arrow::DataType> const& b)
{
    return a->Equals(b);
}
using namespace std::string_literals;
using namespace Catch;
using namespace pd;

TEST_CASE("Test Series constructor with arrow array and boolean input")
{
    // Create an Arrow array to pass as input
    auto arr = arrow::ArrayFromJSON<int>("[1, 2, 3, 4, 5]");

    // Test with isIndex = true
    Series s1(arr, true);
    REQUIRE(s1.name() == "");
    REQUIRE(s1.m_array->Equals(arr));
    REQUIRE(s1.indexArray() == nullptr);

    // Test with isIndex = false
    Series s2(arr, false);
    REQUIRE(s2.name() == "");
    REQUIRE(s2.m_array->Equals(arr));
    REQUIRE_FALSE(s2.indexArray() == nullptr);
}

TEST_CASE("Test Series Initialization", "[series]")
{
    // Test 1: Initialize Series with a vector of ints
    std::vector<int> int_vec = { 1, 2, 3, 4, 5 };
    pd::Series int_series(int_vec);
    REQUIRE(int_series.size() == 5);
    REQUIRE(int_series.dtype()->name() == arrow::int32()->name());
    REQUIRE(int_series.name().empty());
    REQUIRE(int_series.at(0).as<int>() == 1);
    REQUIRE(int_series.at(1).as<int>() == 2);
    REQUIRE(int_series.at(2).as<int>() == 3);
    REQUIRE(int_series.at(3).as<int>() == 4);
    REQUIRE(int_series.at(4).as<int>() == 5);

    // Test 2: Initialize Series with a vector of doubles and a name
    std::vector<double> double_vec = { 1.1, 2.2, 3.3, 4.4, 5.5 };
    pd::Series double_series(double_vec, "my_name");
    REQUIRE(double_series.size() == 5);
    REQUIRE(double_series.dtype()->name() == arrow::float64()->name());
    REQUIRE(double_series.name() == "my_name");
    REQUIRE(double_series.at(0).as<double>() == Approx(1.1));
    REQUIRE(double_series.at(1).as<double>() == Approx(2.2));
    REQUIRE(double_series.at(2).as<double>() == Approx(3.3));
    REQUIRE(double_series.at(3).as<double>() == Approx(4.4));

    // Test 3: Initialize Series with an arrow array
    auto int_array = pd::random::RandomState(1).randint(5);
    pd::Series int_array_series(int_array);
    REQUIRE(int_array_series.size() == 5);
    REQUIRE(int_array_series.dtype() == arrow::int64());
    REQUIRE(int_array_series.name() == "");
    REQUIRE(int_array_series.at(0).scalar->Equals(*arrow::MakeScalar(int_array[0])));
    REQUIRE(int_array_series.at(1).scalar->Equals(*arrow::MakeScalar(int_array[1])));
    REQUIRE(int_array_series.at(2).scalar->Equals(*arrow::MakeScalar(int_array[2])));
    REQUIRE(int_array_series.at(3).scalar->Equals(*arrow::MakeScalar(int_array[3])));
    REQUIRE(int_array_series.at(4).scalar->Equals(*arrow::MakeScalar(int_array[4])));

    // Test 4: Initialize Series with an arrow array and a name
    auto double_array = pd::random::RandomState(1).rand(5);
    pd::Series double_array_series(double_array, "my_name");
    REQUIRE(double_array_series.size() == 5);
    REQUIRE(double_array_series.dtype() == arrow::float64());
    REQUIRE(double_array_series.name() == "my_name");
    REQUIRE(double_array_series.at(0).scalar->Equals(*arrow::MakeScalar(double_array[0])));
    REQUIRE(double_array_series.at(1).scalar->Equals(*arrow::MakeScalar(double_array[1])));
    REQUIRE(double_array_series.at(2).scalar->Equals(*arrow::MakeScalar(double_array[2])));
    REQUIRE(double_array_series.at(3).scalar->Equals(*arrow::MakeScalar(double_array[3])));
    REQUIRE(double_array_series.at(4).scalar->Equals(*arrow::MakeScalar(double_array[4])));

    // Test 5: Initialize Series with a scalar value
    pd::Series scalar_series = pd::Series::MakeScalar(5L);
    REQUIRE(scalar_series.size() == 1);
    REQUIRE(scalar_series.dtype()->Equals(arrow::int64()));
    REQUIRE(scalar_series.name().empty());
    REQUIRE(scalar_series.at(0).scalar->Equals(*arrow::MakeScalar(5L)));
    REQUIRE_FALSE(scalar_series.at(0).scalar->Equals(*arrow::MakeScalar(5)));

    // Test 6: Initialize Series with a scalar value and a name
    pd::Series scalar_series_name = pd::Series::MakeScalar(5.5, "my_name");
    REQUIRE(scalar_series_name.size() == 1);
    REQUIRE(scalar_series_name.dtype()->Equals(arrow::float64()));
    REQUIRE(scalar_series_name.name() == "my_name");
    REQUIRE(scalar_series_name.at(0).scalar->Equals(*arrow::MakeScalar(5.5)));
}

TEST_CASE("Test Series values() and const_ptr() functions")
{
    // Create an Arrow array to pass as input
    auto arr = arrow::ArrayFromJSON<int>("[1, 2, 3, 4, 5]");
    // Create a Series object with the array
    Series s(arr, false);

    // Test values() function
    auto vals = s.values<int>();
    REQUIRE(vals == std::vector<int>({ 1, 2, 3, 4, 5 }));

    // Test const_ptr() function
    auto ptr = s.const_ptr<int>();
    REQUIRE(ptr[3] == 4);
}

TEST_CASE("Test Series view() function")
{
    // Create an Arrow array to pass as input
    auto arr = arrow::ArrayFromJSON<float>("[1.1, 2.2, 3.3, 4.4, 5.5]");
    // Create a Series object with the array
    Series s(arr, false);

    // Test view() function
    auto view = s.view<float>();
    REQUIRE(view->Value(3) == Approx(4.4));
}

TEST_CASE("Test basic operations of Series")
{
    // Create a Series with an array of integers
    std::vector<int> vec = { 1, 2, 3, 4, 5 };
    Series s(vec, "my series");

    // Test the dtype() method
    REQUIRE(s.dtype()->ToString() == "int32");

    // Test the nbytes() method
    REQUIRE(s.nbytes() == vec.size() * sizeof(int));

    // Test the size() method
    REQUIRE(s.size() == vec.size());

    // Test the empty() method
    REQUIRE(!s.empty());

    // Test the name() method
    REQUIRE(s.name() == "my series");

    // Test the add_prefix() method
    s.add_prefix("prefix_");
    REQUIRE(s.name() == "prefix_my series");

    // Test the add_suffix() method
    s.add_suffix("_suffix");
    REQUIRE(s.name() == "prefix_my series_suffix");

    // Test the rename() method
    s.rename("new name");
    REQUIRE(s.name() == "new name");

    // Test the equals() method
    std::vector<int> vec2 = { 1, 2, 3, 4, 5 };
    Series s3(vec2);
    REQUIRE(s3.name() == "");
    REQUIRE(s3.is<int>());
    REQUIRE(s3.dtype()->Equals(arrow::int32()));
    REQUIRE(s3.nbytes() == (sizeof(int) * vec2.size()));
    REQUIRE(s3.size() == vec2.size());
    REQUIRE(s3.empty() == false);
    REQUIRE(s3.values<int>() == vec2);

    // Test name modification
    s3.rename("my_series");
    REQUIRE(s3.name() == "my_series");
    s3.add_prefix("prefix_");
    REQUIRE(s3.name() == "prefix_my_series");
    s3.add_suffix("_suffix");
    REQUIRE(s3.name() == "prefix_my_series_suffix");

    // Test equality and approximation
    std::vector<int> vec3 = { 1, 2, 3, 4, 5 };
    REQUIRE(s3.equals(vec3));
    REQUIRE(s3.approx_equals(vec3));

    std::vector<int> vec4 = { 1, 2, 3, 4, 6 };
    REQUIRE_FALSE(s3.approx_equals(vec4));
}

TEST_CASE("Test Series equals method", "[series]")
{
    std::vector<int> vec1 = { 1, 2, 3, 4, 5 };
    std::vector<int> vec2 = { 1, 2, 3, 4, 5 };
    std::vector<int> vec3 = { 1, 2, 3, 4 };
    std::vector<int> vec4 = { 1, 2, 3, 4, 6 };
    Series s1(vec1);
    REQUIRE(s1.equals(vec2));
    REQUIRE_FALSE(s1.equals(vec3));
    REQUIRE_FALSE(s1.equals(vec4));
}

TEST_CASE("Test Series approx_equals method", "[series]")
{
    std::vector<double> vec1 = { 1.0, 2.0, 3.0, 4.0, 5.0 };
    std::vector<double> vec2 = { 1.0, 2.0, 3.0, 4.0, 5.1 };
    std::vector<double> vec3 = { 1.0, 2.0, 3.0, 4.0, 5.5 };
    Series s1(vec1);
    REQUIRE_FALSE(s1.approx_equals(vec2));
    REQUIRE_FALSE(s1.approx_equals(vec3));
}

TEST_CASE("Test Series::index() function")
{
    std::vector<int> vec1 = { 1, 2, 3, 4, 5 };

    SECTION("Series as non-index")
    {
        auto array1 = arrow::ArrayT<int>::Make(vec1);
        Series s1(array1, false);

        // Test with valid search value
        REQUIRE(s1.index(pd::Scalar{3}) == 2);

        // Test with invalid search value
        REQUIRE(s1.index(pd::Scalar{6}) == -1);

        REQUIRE(s1.getIndexer().empty());
        REQUIRE_FALSE(s1.indexArray() == nullptr);
    }

    SECTION("Series as index")
    {
        vec1 = { 1, 2, 3, 6, 7 };
        auto array1 = arrow::ArrayT<int>::Make(vec1);
        Series s1(array1, true);

        auto indexer = s1.getIndexer();
        REQUIRE_FALSE(indexer.empty());
        REQUIRE(s1.indexArray() == nullptr);

        pd::Scalar scalar(3);
        pd::Scalar scalar1(-1);
        REQUIRE(indexer.contains(scalar.scalar));
        REQUIRE(s1.index(scalar) == 2);
        REQUIRE(s1.index(scalar1) == -1);
    }
}

TEST_CASE("Test Series::is_unique() function")
{
    std::vector<int> vec1 = { 1, 2, 3, 4, 5 };
    auto array1 = arrow::ArrayT<int>::Make(vec1);
    Series s1(array1, true);
    REQUIRE(s1.is_unique() == true);

    std::vector<int> vec2 = { 1, 2, 3, 4, 5, 5 };
    auto array2 = arrow::ArrayT<int>::Make(vec2);
    Series s2(array2, true);
    REQUIRE(s2.is_unique() == false);
}

TEST_CASE("Test Series::is_unique() and value_counts() functions")
{
    std::vector<int> vec1 = { 1, 2, 3, 2, 5, 2, 7 };
    auto array1 = arrow::ArrayT<int>::Make(vec1);
    Series s1(array1, true);

    REQUIRE(s1.is_unique() == false);

    auto result = s1.value_counts();
    REQUIRE(result.shape() == std::array<int64_t, 2>{ 5, 2 });

    REQUIRE(result["values"][0] == 1);
    REQUIRE(result["counts"][0] == 1L);

    REQUIRE(result["values"][1] == 2);
    REQUIRE(result["counts"][1] == 3L);

    REQUIRE(result["values"][2] == 3);
    REQUIRE(result["counts"][2] == 1L);

    REQUIRE(result["values"][3] == 5);
    REQUIRE(result["counts"][3] == 1L);

    REQUIRE(result["values"][4] == 7);
    REQUIRE(result["counts"][4] == 1L);

    REQUIRE(result.at(0, 0) == 1);
    REQUIRE(result.at(0, 1) == 1L);
    REQUIRE(result.at(1, 0) == 2);
    REQUIRE(result.at(1, 1) == 3L);
    REQUIRE(result.at(2, 0) == 3);
    REQUIRE(result.at(2, 1) == 1L);
    REQUIRE(result.at(3, 0) == 5);
    REQUIRE(result.at(3, 1) == 1L);
    REQUIRE(result.at(4, 0) == 7);
    REQUIRE(result.at(4, 1) == 1L);

    REQUIRE_THROWS_AS(result.at(1, 4), std::runtime_error);
    REQUIRE_THROWS_AS(result.at(5, 1), std::runtime_error);
    REQUIRE_THROWS_AS(result.at(5, 5), std::runtime_error);

    auto dict_result = s1.dictionary_encode();
    REQUIRE(dict_result->type_id() == arrow::Type::DICTIONARY);
    REQUIRE(dict_result->length() == 7);
    REQUIRE(dict_result->dictionary()->length() == 5);
}

TEST_CASE("Test Series unique(), drop_na(), and indices_nonzero() methods")
{
    std::vector<int> vec1 = { 1, 2, 3, 4, 5, 1, 2, 3, 4, 5, 6 };
    auto array1 = arrow::ArrayT<int>::Make(vec1);
    Series s1(array1, true);

    std::vector<int> vec2 = { 1, 2, 3, 4, 5, 1, 2, 3, 4, 5, 6, -1 };
    auto array2 =
        arrow::ArrayT<int>::Make(vec2, { true, true, true, true, true, true, true, true, true, true, true, false });
    Series s2(array2, true);

    SECTION("Test drop_na() method")
    {
        auto result = s2.drop_na();

        REQUIRE(result.size() == 11);
        REQUIRE(result.at(0) == 1);
        REQUIRE(result.at(1) == 2);
        REQUIRE(result.at(2) == 3);
        REQUIRE(result.at(3) == 4);
        REQUIRE(result.at(4) == 5);
        REQUIRE(result.at(5) == 1);
        REQUIRE(result.at(6) == 2);
        REQUIRE(result.at(7) == 3);
        REQUIRE(result.at(8) == 4);
        REQUIRE(result.at(9) == 5);
        REQUIRE(result.at(10) == 6);
    }

    std::vector<int> vec3 = { 0, 0, 0, 1, 2, 0, 3, 0, 4, 5, 0 };
    auto array3 = arrow::ArrayT<int>::Make(vec3);
    Series s3(array3, true);

    SECTION("Test indices_nonzero() method")
    {
        auto result = s3.indices_nonzero();

        INFO(result);

        REQUIRE(result.size() == 5);
        REQUIRE(result.at(0) == 3UL);
        REQUIRE(result.at(1) == 4UL);
        REQUIRE(result.at(2) == 6UL);
        REQUIRE(result.at(3) == 8UL);
        REQUIRE(result.at(4) == 9UL);
    }
}

TEST_CASE("Test Series::ffill() and Series::bfill() functions")
{
    std::vector<int> vec1 = { 1, 2, 3, 4, 5, 0, 7 };
    std::vector<bool> mask = { true, true, true, true, true, false, true };
    auto array1 = arrow::ArrayT<int>::Make(vec1, mask);
    Series s1_masked(array1, true);

    SECTION("Test ffill()")
    {
        auto result = s1_masked.ffill();

        REQUIRE(result.size() == 7);
        REQUIRE(result.at(0) == 1);
        REQUIRE(result.at(1) == 2);
        REQUIRE(result.at(2) == 3);
        REQUIRE(result.at(3) == 4);
        REQUIRE(result.at(4) == 5);
        REQUIRE(result.at(5) == 5); // should fill with last valid value
        REQUIRE(result.at(6) == 7);
    }

    SECTION("Test bfill()")
    {
        auto result = s1_masked.bfill();

        REQUIRE(result.size() == 7);
        REQUIRE(result.at(0) == 1);
        REQUIRE(result.at(1) == 2);
        REQUIRE(result.at(2) == 3);
        REQUIRE(result.at(3) == 4);
        REQUIRE(result.at(4) == 5);
        REQUIRE(result.at(5) == 7); // should fill with next valid value
        REQUIRE(result.at(6) == 7);
    }
}

TEST_CASE("Test Series::replace_with_mask() function")
{
    SECTION("Test with valid params")
    {
        std::vector<int> vec1 = { 1, 2, 3, 4, 5, 6, 7 };
        auto array1 = arrow::ArrayT<int>::Make(vec1);
        Series s1(array1, true);

        std::vector<bool> mask = { true, true, true, false, false, true, true };
        auto array2 = arrow::ArrayT<bool>::Make(mask);
        Series mask_series(array2, true);

        std::vector<int> vec2 = { 10, 20, 30, 40, 50, 60, 70 };
        auto array3 = arrow::ArrayT<int>::Make(vec2);
        Series other_series(array3, true);

        auto result = s1.replace_with_mask(mask_series, other_series);
        INFO(result);
        REQUIRE(result.size() == 7);
        REQUIRE(result.at(0) == 10);
        REQUIRE(result.at(1) == 20);
        REQUIRE(result.at(2) == 30);
        REQUIRE(result.at(3) == 4);
        REQUIRE(result.at(4) == 5);
        REQUIRE(result.at(5) == 40); // WIERD HOW IT WORKS LIKE THIS
        REQUIRE(result.at(6) == 50);
    }

    SECTION("Test with different size mask and other series")
    {
        std::vector<int> vec1 = { 1, 2, 3, 4, 5 };
        auto array1 = arrow::ArrayT<int>::Make(vec1);
        Series s1(array1, true);


        std::vector<bool> mask = { true, true, true, false, false };
        auto array2 = arrow::ArrayT<bool>::Make(mask);
        Series mask_series(array2, true);

        std::vector<int> vec2 = { 10, 20, 30 };
        auto array3 = arrow::ArrayT<int>::Make(vec2);
        Series other_series(array3, true);

        REQUIRE_THROWS_AS(s1.replace_with_mask(mask_series, other_series), std::runtime_error);
    }

    SECTION("Test with different data types")
    {
        std::vector<int> vec1 = { 1, 2, 3, 4, 5 };
        auto array1 = arrow::ArrayT<int>::Make(vec1);
        Series s1(array1, true);


        std::vector<bool> mask = { true, true, true, false, false };
        auto array2 = arrow::ArrayT<bool>::Make(mask);
        Series mask_series(array2, true);

        std::vector<float> vec2 = { 10.5, 20.5, 30.5, 40.5, 50.5 };
        auto array3 = arrow::ArrayT<float>::Make(vec2);
        Series other_series(array3, true);

        REQUIRE_THROWS_AS(s1.replace_with_mask(mask_series, other_series), std::runtime_error);
    }

    SECTION("Test with same size but mask contains NA")
    {
        std::vector<int> vec1 = { 1, 2, 3, 4, 5 };
        std::vector<bool> mask = { true, true, false, false, true };
        std::vector<int> vec2 = { 10, 20, 30, 40, 50 };

        auto array1 = arrow::ArrayT<int>::Make(vec1);
        auto array2 = arrow::ArrayT<bool>::Make(mask, { true, true, false, true, true });
        auto array3 = arrow::ArrayT<int>::Make(vec2);

        Series s1(array1, true);
        Series mask_series(array2, true);
        Series other_series(array3, true);

        auto result = s1.replace_with_mask(mask_series, other_series);
        INFO(result);
        REQUIRE(result.size() == 5);
        REQUIRE(result.at(0) == 10);
        REQUIRE(result.at(1) == 20);
        REQUIRE_FALSE(result.at(2).isValid());
        REQUIRE(result.at(3) == 4);
        REQUIRE(result.at(4) == 30);
    }

    SECTION("Test with different size")
    {
        std::vector<int> vec1 = { 1, 2, 3, 4, 5 };
        std::vector<bool> mask = { true, true, true };
        std::vector<int> vec2 = { 10, 20, 30 };

        auto array1 = arrow::ArrayT<int>::Make(vec1);
        auto array2 = arrow::ArrayT<bool>::Make(mask);
        auto array3 = arrow::ArrayT<int>::Make(vec2);

        Series s1(array1, true);
        Series mask_series(array2, true);
        Series other_series(array3, true);

        REQUIRE_THROWS_AS(s1.replace_with_mask(mask_series, other_series), std::runtime_error);
    }

    SECTION("Test with different types")
    {
        std::vector<int> vec1 = { 1, 2, 3, 4, 5 };
        std::vector<bool> mask = { true, true, true, true, true };
        std::vector<double> vec2 = { 10.5, 20.5, 30.5, 40.5, 50.5 };

        auto array1 = arrow::ArrayT<int>::Make(vec1);
        auto array2 = arrow::ArrayT<bool>::Make(mask);
        auto array3 = arrow::ArrayT<double>::Make(vec2);

        Series s1(array1, true);
        Series mask_series(array2, true);
        Series other_series(array3, true);

        REQUIRE_THROWS_AS(s1.replace_with_mask(mask_series, other_series), std::runtime_error);
    }
}

TEST_CASE("Test Series::if_else() functions")
{
    std::vector<int> vec1 = { 1, 2, 3, 4, 5 };
    std::vector<bool> mask = { true, true, false, false, true };
    std::vector<int> vec2 = { 10, 20, 30, 40, 50 };

    auto array1 = arrow::ArrayT<int>::Make(vec1);
    auto array2 = arrow::ArrayT<bool>::Make(mask);
    auto array3 = arrow::ArrayT<int>::Make(vec2);

    Series s1(array1, true);
    Series mask_series(array2, true);
    Series other_series(array3, true);

    Scalar other_scalar(100L);

    SECTION("Test with Series for other values")
    {
        auto result = s1.if_else(mask_series, other_series);
        INFO(result);
        REQUIRE(result.size() == 5);
        REQUIRE(result.at(0) == 1);
        REQUIRE(result.at(1) == 2);
        REQUIRE(result.at(2) == 30);
        REQUIRE(result.at(3) == 40);
        REQUIRE(result.at(4) == 5);
    }

    SECTION("Test with Scalar for other values")
    {
        auto result = s1.if_else(mask_series, other_scalar);
        INFO(result);

        REQUIRE(result.size() == 5);
        REQUIRE(result.at(0) == 1L);
        REQUIRE(result.at(1) == 2L);
        REQUIRE(result.at(2) == 100L);
        REQUIRE(result.at(3) == 100L);
        REQUIRE(result.at(4) == 5L);
    }
}

TEST_CASE("Test Series::is_null() function")
{

    SECTION("Test with no null values")
    {
        std::vector<int> vec1 = { 1, 2, 3, 4, 5 };
        std::vector<bool> mask = { true, true, true, true, true };
        auto array1 = arrow::ArrayT<int>::Make(vec1, mask);
        Series s1(array1, true);

        auto result = s1.is_null();
        REQUIRE(result.size() == 5);
        for (int i = 0; i < result.size(); i++)
        {
            REQUIRE_FALSE(result.at(i).as<bool>());
        }
    }

    SECTION("Test with null values")
    {
        std::vector<int> vec2 = { 1, 2, 3, 4, 5 };
        std::vector<bool> mask2 = { true, true, true, true, false };
        auto array2 = arrow::ArrayT<int>::Make(vec2, mask2);
        Series s2(array2, true);

        auto result = s2.is_null();
        REQUIRE(result.size() == 5);
        for (int i = 0; i < result.size() - 1; i++)
        {
            INFO(i << result.at(i));
            REQUIRE_FALSE(result.at(i).as<bool>());
        }
        REQUIRE(result.at(4).as<bool>());
    }
}

TEST_CASE("Test Series::is_finite() function")
{
    std::vector<double> vec1 = { 1.0, 2.0, 3.0, 4.0, 5 };
    auto array1 = arrow::ArrayT<double>::Make(vec1);
    Series s1(array1, true);


    SECTION("Test with all finite values")
    {
        auto result = s1.is_finite();

        REQUIRE(result.size() == 5);
        REQUIRE(result.at(0) == true);
        REQUIRE(result.at(1) == true);
        REQUIRE(result.at(2) == true);
        REQUIRE(result.at(3) == true);
        REQUIRE(result.at(4) == true);
    }

    SECTION("Test with one infinite value")
    {
        vec1[2] = std::numeric_limits<double>::infinity();
        auto array1 = arrow::ArrayT<double>::Make(vec1);
        Series s1(array1, true);
        auto result = s1.is_finite();

        REQUIRE(result.size() == 5);
        REQUIRE(result.at(0) == true);
        REQUIRE(result.at(1) == true);
        REQUIRE(result.at(2) == false);
        REQUIRE(result.at(3) == true);
        REQUIRE(result.at(4) == true);
    }
}

TEST_CASE("Test Series::cast() function")
{
    std::vector<double> vec1 = { 1.0, 2.0, 3.0, 4.0, 5.0 };
    auto array1 = arrow::ArrayT<double>::Make(vec1);
    Series s1(array1, true);

    SECTION("Test with casting to int")
    {
        auto result = s1.cast<int>();
        REQUIRE(result.size() == 5);
        REQUIRE(result.at(0) == 1);
        REQUIRE(result.at(1) == 2);
        REQUIRE(result.at(2) == 3);
        REQUIRE(result.at(3) == 4);
        REQUIRE(result.at(4) == 5);
    }

    SECTION("Test with casting to bool")
    {
        auto result = s1.cast<bool>();
        REQUIRE(result.size() == 5);
        REQUIRE(result.at(0) == true);
        REQUIRE(result.at(1) == true);
        REQUIRE(result.at(2) == true);
        REQUIRE(result.at(3) == true);
        REQUIRE(result.at(4) == true);
    }

    SECTION("Test with safe casting")
    {
        auto result = s1.cast<bool>(true);
        REQUIRE(result.size() == 5);
        REQUIRE(result.at(0) == true);
        REQUIRE(result.at(1) == true);
        REQUIRE(result.at(2) == true);
        REQUIRE(result.at(3) == true);
        REQUIRE(result.at(4) == true);
    }
}

TEST_CASE("Test Series::shift() and Series::pct_change() functions", "[shift_pct]")
{
    std::vector<double> vec1 = { 1.0, 2.0, 3.0, 4.0, 5.0 };
    auto array1 = arrow::ArrayT<double>::Make(vec1);
    Series s1(array1, range(0L, 5));

    SECTION("Test shift() with default options")
    {
        auto result = s1.shift();
        REQUIRE(result.size() == 5);
        REQUIRE_FALSE(result.at(0).isValid());
        REQUIRE(result.at(1) == 1.0);
        REQUIRE(result.at(2) == 2.0);
        REQUIRE(result.at(3) == 3.0);
        REQUIRE(result.at(4) == 4.0);
    }

    SECTION("Test shift() with custom shift value and fill value")
    {
        std::shared_ptr<arrow::Scalar> fill_value = arrow::MakeScalar(0.0);
        auto result = s1.shift(-2, pd::Scalar{fill_value});
        REQUIRE(result.size() == 5);
        REQUIRE(result.at(0) == 3.0);
        REQUIRE(result.at(1) == 4.0);
        REQUIRE(result.at(2) == 5.0);
        REQUIRE(result.at(3) == 0.0);
        REQUIRE(result.at(4) == 0.0);
    }

    SECTION("Test pct_change() with default periods")
    {
        auto result = s1.pct_change();
        REQUIRE(result.size() == 5);
        REQUIRE_FALSE(result.at(0).isValid());
        REQUIRE(result.at(1) == 1.0);
        REQUIRE(result.at(2).as<double>() == 0.5);
        REQUIRE(result.at(3).as<double>() == Catch::Approx(0.3333333333).epsilon(0.01));
        REQUIRE(result.at(4).as<double>() == Catch::Approx(0.25).epsilon(0.01));
    }

    SECTION("Test with negative shift value")
    {
        auto result = s1.shift(-1);

        REQUIRE(result.size() == 5);
        REQUIRE(result.at(0).as<double>() == Catch::Approx(2));
        REQUIRE(result.at(1).as<double>() == Catch::Approx(3));
        REQUIRE(result.at(2).as<double>() == Catch::Approx(4));
        REQUIRE(result.at(3).as<double>() == Catch::Approx(5));
        REQUIRE(result.is_valid(4) == false);
    }

    SECTION("Test with custom fill value")
    {
        auto fill_value = arrow::MakeScalar(10.0);
        auto result = s1.shift(-1, pd::Scalar{fill_value});

        REQUIRE(result.size() == 5);
        REQUIRE(result.at(0).as<double>() == Catch::Approx(2));
        REQUIRE(result.at(1).as<double>() == Catch::Approx(3));
        REQUIRE(result.at(2).as<double>() == Catch::Approx(4));
        REQUIRE(result.at(3).as<double>() == Catch::Approx(5));
        REQUIRE(result.at(4).as<double>() == Catch::Approx(10));
    }
}

TEST_CASE("Test Series::append() function")
{
    SECTION("Test with same index type and unique index")
    {
        std::vector<int> vec1 = { 1, 2, 3, 4, 5 };
        std::vector<int> vec2 = { 6, 7, 8, 9, 10 };
        std::vector<int> vec3 = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };

        auto array1 = arrow::ArrayT<int>::Make(vec1);
        auto array2 = arrow::ArrayT<int>::Make(vec2);

        Series s1(array1, true);
        Series s2(array2, true);

        auto result = s1.append(s2);
        REQUIRE(result.size() == 10);
        REQUIRE(result.IsIndexArray() == true);

        auto indexer = result.getIndexer();
        for (int i = 0; i < indexer.size(); i++)
        {
            REQUIRE(indexer.at(arrow::MakeScalar(vec3[i])->GetSharedPtr()) == i);
        }
        for (int i = 0; i < 10; i++)
        {
            REQUIRE(result.at(i) == vec3[i]);
        }
    }

    SECTION("Test with same index type but duplicate index")
    {
        std::vector<int> vec1 = { 1, 2, 3, 4, 5 };
        std::vector<int> vec2 = { 3, 4, 5, 6, 7 };

        auto array1 = arrow::ArrayT<int>::Make(vec1);
        auto array2 = arrow::ArrayT<int>::Make(vec2);

        Series s1(array1, true);
        Series s2(array2, true);

        CHECK_THROWS_AS(s1.append(s2), std::runtime_error);
    }

    SECTION("Test with different arrays")
    {
        std::vector<int> vec1 = { 1, 2, 3, 4, 5 };
        std::vector<int> vec2 = { 6, 7, 8, 9, 10 };

        auto array1 = arrow::ArrayT<int>::Make(vec1);
        auto array2 = arrow::ArrayT<int>::Make(vec2);

        Series s1(array1, nullptr);
        Series to_append(array2, nullptr);

        auto result = s1.append(to_append);
        REQUIRE(result.size() == 10);
        REQUIRE(result.at(0) == 1);
        REQUIRE(result.at(1) == 2);
        REQUIRE(result.at(2) == 3);
        REQUIRE(result.at(3) == 4);
        REQUIRE(result.at(4) == 5);
        REQUIRE(result.at(5) == 6);
        REQUIRE(result.at(6) == 7);
        REQUIRE(result.at(7) == 8);
        REQUIRE(result.at(8) == 9);
        REQUIRE(result.at(9) == 10);
    }

    SECTION("Test with different index type but ignore_index=true")
    {
        std::vector<int> vec1 = { 1, 2, 3, 4, 5 };
        std::vector<int> vec2 = { 6, 7, 8, 9, 10 };

        auto array1 = arrow::ArrayT<int>::Make(vec1);
        auto array2 = arrow::ArrayT<int>::Make(vec2);

        Series s1(array1, true);
        Series to_append(array2, false);

        auto result = s1.append(to_append, true);
        INFO(result);
        REQUIRE(result.size() == 10);
        REQUIRE(result.at(0) == 1);
        REQUIRE(result.at(1) == 2);
        REQUIRE(result.at(2) == 3);
        REQUIRE(result.at(3) == 4);
        REQUIRE(result.at(4) == 5);
        REQUIRE(result.at(5) == 6);
        REQUIRE(result.at(6) == 7);
        REQUIRE(result.at(7) == 8);
        REQUIRE(result.at(8) == 9);
        REQUIRE(result.at(9) == 10);
    }

    SECTION("Test with different index type but ignore_index=false")
    {
        std::vector<int> vec1 = { 1, 2, 3, 4, 5 };
        std::vector<int> vec2 = { 6, 7, 8, 9, 10 };

        auto array1 = arrow::ArrayT<int>::Make(vec1);
        auto array2 = arrow::ArrayT<int>::Make(vec2);

        Series s1(array1, true);
        Series to_append(array2, false);

        REQUIRE_THROWS(s1.append(to_append, false));
    }

    SECTION("Test with different index type but ignore_index=false")
    {
        std::vector<int> vec1 = { 1, 2, 3, 4, 5 };
        std::vector<int> vec2 = { 6, 7, 8, 9, 10 };
        std::vector<int> vec3 = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
        std::vector<int> vec4 = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };

        auto array1 = arrow::ArrayT<int>::Make(vec1);
        auto array2 = arrow::ArrayT<int>::Make(vec2);
        auto array3 = arrow::ArrayT<int>::Make(vec3);
        auto array4 = arrow::ArrayT<int>::Make(vec4);

        Series s1(array1, true);
        Series to_append(array2, false);
        REQUIRE_THROWS(s1.append(to_append, false));

        s1 = Series(array1, nullptr, "", true);
        to_append = Series(array2, array4);

        REQUIRE_THROWS(s1.append(to_append, false));
    }

    SECTION("Test with same index type and indexer unique")
    {
        auto array1 = arrow::ArrayT<int>::Make({ 1, 2, 3, 4, 5 });
        auto array2 = arrow::ArrayT<int>::Make({ 6, 7, 8, 9, 10 });
        auto array3 = arrow::ArrayT<int>::Make({ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 });

        Series s1(array1, true);
        Series to_append(array2, true);
        Series to_append2(array3, true);

        auto result = s1.append(to_append2, true);
        REQUIRE(result.size() == 15);
        REQUIRE(result.at(0) == 1);
        REQUIRE(result.at(1) == 2);
        REQUIRE(result.at(2) == 3);
        REQUIRE(result.at(3) == 4);
        REQUIRE(result.at(4) == 5);
        REQUIRE(result.at(5) == 1);
        REQUIRE(result.at(6) == 2);
        REQUIRE(result.at(7) == 3);
        REQUIRE(result.at(8) == 4);
        REQUIRE(result.at(9) == 5);
        REQUIRE(result.at(10) == 6);
        REQUIRE(result.at(11) == 7);
        REQUIRE(result.at(12) == 8);
        REQUIRE(result.at(13) == 9);
        REQUIRE(result.at(14) == 10);
    }
}

TEST_CASE("StringLike")
{
    using namespace std::string_literals;
    SECTION("Test Series::str() function")
    {
        std::vector<std::string> vec1 = { "a", "b", "c", "d", "e" };
        auto array1 = arrow::ArrayT<std::string>::Make(vec1);
        Series s1(array1, false);
        auto result = s1.str();
        REQUIRE(result.size() == 5);
        REQUIRE(result.at(0) == "a"s);
        REQUIRE(result.at(1) == "b"s);
        REQUIRE(result.at(2) == "c"s);
        REQUIRE(result.at(3) == "d"s);
        REQUIRE(result.at(4) == "e"s);
    }

    SECTION("Test StringLike::length() function")
    {
        std::vector<std::string> vec1 = { "a", "b", "c", "d", "e" };
        auto array1 = arrow::ArrayT<std::string>::Make(vec1);
        Series s1(array1, false);
        auto result = s1.str().utf8_length();
        REQUIRE(result.size() == 5);
        REQUIRE(result.at(0) == 1);
        REQUIRE(result.at(1) == 1);
        REQUIRE(result.at(2) == 1);
        REQUIRE(result.at(3) == 1);
        REQUIRE(result.at(4) == 1);
    }

    SECTION("Test StringLike::upper() function")
    {
        std::vector<std::string> vec1 = { "a", "b", "c", "d", "e" };
        auto array1 = arrow::ArrayT<std::string>::Make(vec1);
        Series s1(array1, false);
        auto result = s1.str().utf8_upper();
        REQUIRE(result.size() == 5);
        REQUIRE(result.at(0) == "A"s);
        REQUIRE(result.at(1) == "B"s);
        REQUIRE(result.at(2) == "C"s);
        REQUIRE(result.at(3) == "D"s);
        REQUIRE(result.at(4) == "E"s);
    }

    SECTION("Test StringLike::lower() function")
    {
        std::vector<std::string> vec1 = { "A", "B", "C", "D", "E" };
        auto array1 = arrow::ArrayT<std::string>::Make(vec1);
        Series s1(array1, false);
        auto result = s1.str().utf8_lower();
        REQUIRE(result.size() == 5);
        REQUIRE(result.at(0) == "a"s);
        REQUIRE(result.at(1) == "b"s);
        REQUIRE(result.at(2) == "c"s);
        REQUIRE(result.at(3) == "d"s);
        REQUIRE(result.at(4) == "e"s);
    }

    SECTION("Test StringLike::capitalize() function")
    {
        std::vector<std::string> vec1 = { "ae", "be", "ce", "de", "ex" };
        auto array1 = arrow::ArrayT<std::string>::Make(vec1);
        Series s2(array1, false);
        auto result = s2.str().utf8_capitalize();
        REQUIRE(result.at(0) == "Ae"s);
        REQUIRE(result.at(1) == "Be"s);
        REQUIRE(result.at(2) == "Ce"s);
        REQUIRE(result.at(3) == "De"s);
        REQUIRE(result.at(4) == "Ex"s);
    }

    SECTION("Test with string array")
    {
        std::vector<std::string> vec2 = { "a", "b", "c", "d", "e" };
        auto array2 = arrow::ArrayT<std::string>::Make(vec2);
        Series s2(array2, false);

        auto result = s2.str();
        REQUIRE(result.at(0) == "a"s);
        REQUIRE(result.at(1) == "b"s);
        REQUIRE(result.at(2) == "c"s);
        REQUIRE(result.at(3) == "d"s);
        REQUIRE(result.at(4) == "e"s);
    }


    SECTION("Test with double array")
    {
        std::vector<double> vec3 = { 1.1, 2.2, 3.3, 4.4, 5.5 };
        auto array3 = arrow::ArrayT<double>::Make(vec3);
        Series s3(array3, false);

        auto result = s3.str();
        REQUIRE(result.at(0) == "1.1"s);
        REQUIRE(result.at(1) == "2.2"s);
        REQUIRE(result.at(2) == "3.3"s);
        REQUIRE(result.at(3) == "4.4"s);
        REQUIRE(result.at(4) == "5.5"s);
    }

    SECTION("Test with bool array")
    {
        std::vector<bool> vec4 = { true, false, true, false, true };
        auto array4 = arrow::ArrayT<bool>::Make(vec4);
        Series s4(array4, false);

        auto result = s4.str();
        INFO(result);
        REQUIRE(result.at(0) == "true"s);
        REQUIRE(result.at(1) == "false"s);
        REQUIRE(result.at(2) == "true"s);
        REQUIRE(result.at(3) == "false"s);
        REQUIRE(result.at(4) == "true"s);
    }

    SECTION("Test with int array")
    {
        std::vector<int> vec4 = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 20 };
        auto array4 = arrow::ArrayT<int>::Make(vec4);
        Series s1(array4, false);
        auto result = s1.str();
        REQUIRE(result.at(0) == "1"s);
        REQUIRE(result.at(1) == "2"s);
        REQUIRE(result.at(2) == "3"s);
        REQUIRE(result.at(3) == "4"s);
        REQUIRE(result.at(4) == "5"s);
        REQUIRE(result.at(5) == "6"s);
        REQUIRE(result.at(6) == "7"s);
        REQUIRE(result.at(7) == "8"s);
        REQUIRE(result.at(8) == "9"s);
        REQUIRE(result.at(9) == "20"s);
    }
}

TEST_CASE("Intersection of Two Series Index")
{
    std::vector<int> vec1 = { 1, 2, 3, 4, 5 };
    std::vector<int> vec2 = { 3, 4, 5, 6, 7 };
    auto array1 = arrow::ArrayT<int>::Make(vec1);
    auto array2 = arrow::ArrayT<int>::Make(vec2);
    Series s1(array1, true);
    Series s2(array2, true);
    auto result = s1.intersection(s2);

    INFO(result);
    REQUIRE(result.size() == 3);
    REQUIRE(result.at(0) == 3);
    REQUIRE(result.at(1) == 4);
    REQUIRE(result.at(2) == 5);
}

TEST_CASE("Union of two index series")
{
    std::vector<int> vec1 = { 1, 2, 3, 4 };
    std::vector<int> vec2 = { 3, 4, 5, 6 };
    auto array1 = arrow::ArrayT<int>::Make(vec1);
    auto array2 = arrow::ArrayT<int>::Make(vec2);
    Series s1(array1, true);
    Series s2(array2, true);
    auto result = s1.union_(s2);

    INFO(result);
    REQUIRE(result.size() == 6);
    REQUIRE(result.at(0) == 1);
    REQUIRE(result.at(1) == 2);
    REQUIRE(result.at(2) == 3);
    REQUIRE(result.at(3) == 4);
    REQUIRE(result.at(4) == 5);
    REQUIRE(result.at(5) == 6);

    REQUIRE_THROWS(s1.union_(s1.shift()));
}





TEST_CASE("Test Series::argsort() and Series::sort() functions")
{
    std::vector<int> vec = { 5, 3, 2, 4, 1 };
    auto array = arrow::ArrayT<int>::Make(vec);
    Series s(array, true);

    SECTION("Test sort with ascending order")
    {
        auto result1 = s.argsort();
        REQUIRE(result1.size() == 5);
        INFO(result1);
        REQUIRE(result1.at(0) == 4UL);
        REQUIRE(result1.at(1) == 2UL);
        REQUIRE(result1.at(2) == 1UL);
        REQUIRE(result1.at(3) == 3UL);
        REQUIRE(result1.at(4) == 0UL);
    }

    SECTION("Test sort with descending order")
    {
        auto result2 = s.argsort(false);
        INFO(result2);
        REQUIRE(result2.size() == 5);
        REQUIRE(result2.at(0) == 0UL);
        REQUIRE(result2.at(1) == 3UL);
        REQUIRE(result2.at(2) == 1UL);
        REQUIRE(result2.at(3) == 2UL);
        REQUIRE(result2.at(4) == 4UL);
    }

    SECTION("Test sort with ascending order")
    {
        auto result3 = Series(s.sort()[0], true);
        INFO(result3);
        REQUIRE(result3.size() == 5);
        REQUIRE(result3.at(0) == 1);
        REQUIRE(result3.at(1) == 2);
        REQUIRE(result3.at(2) == 3);
        REQUIRE(result3.at(3) == 4);
        REQUIRE(result3.at(4) == 5);
    }

    SECTION("Test sort with descending order")
    {
        auto result4 = Series(s.sort(false)[0], true);
        INFO(result4);
        REQUIRE(result4.size() == 5);
        REQUIRE(result4.at(0) == 5);
        REQUIRE(result4.at(1) == 4);
        REQUIRE(result4.at(2) == 3);
        REQUIRE(result4.at(3) == 2);
        REQUIRE(result4.at(4) == 1);
    }
}

TEST_CASE("Test Series::nth_element() function")
{

    SECTION("Correct Operation")
    {
        std::vector<int> vec1 = { 1, 3, 2, 5, 4 };
        auto array1 = arrow::ArrayT<int>::Make(vec1);
        Series s1(array1, true);

        auto result = s1.nth_element(2);
        REQUIRE(result.size() == 5);
        INFO(result);
        REQUIRE(result.at(0) == 0UL);
        REQUIRE(result.at(1) == 2UL);
        REQUIRE(result.at(2) == 1UL);
        REQUIRE(result.at(3) == 3UL);
        REQUIRE(result.at(4) == 4UL);
    }
}

TEST_CASE("Test Series::cov() and Series::corr() functions", "[.cov_corr]")
{
    std::vector<double> vec1 = { 1.0, 2.0, 3.0, 4.0, 5.0 };
    std::vector<double> vec2 = { 2.0, 3.0, 4.0, 5.0, 6.0 };

    auto array1 = arrow::ArrayT<double>::Make(vec1);
    auto array2 = arrow::ArrayT<double>::Make(vec2);

    Series s1(array1, true);
    Series s2(array2, true);

    auto cov_result = s1.cov(s2);
    REQUIRE(cov_result == Approx(2.5));

    auto corr_result = s1.corr(s2);
    REQUIRE(corr_result == Approx(0.9999999999999999));

    REQUIRE_THROWS_AS(s1.corr(s2, CorrelationType::Kendall), std::runtime_error);

    REQUIRE_THROWS_AS(s1.corr(s2, CorrelationType::Spearman), std::runtime_error);
}

TEST_CASE("Test Series::ewm with mean")
{
    auto result =
        Series(std::vector{ 1.0, 2.0, 3.0, 4.0, 5.0 }).ewm(pd::EWMAgg::Mean, 0.5, pd::EWMAlphaType::CenterOfMass, true);

    REQUIRE(result.size() == 5);
    INFO(result);
    REQUIRE(result.at(0).as<double>() == Catch::Approx(1));
    REQUIRE(result.at(1).as<double>() == Catch::Approx(1.75).epsilon(0.01));
    REQUIRE(result.at(2).as<double>() == Catch::Approx(2.615).epsilon(0.01));
    REQUIRE(result.at(3).as<double>() == Catch::Approx(3.55).epsilon(0.01));
    REQUIRE(result.at(4).as<double>() == Catch::Approx(4.53).epsilon(0.01));

    result = Series(std::vector{ 1.0, 2.0, 3.0, 4.0, 5.0 })
                 .ewm(pd::EWMAgg::Mean, 0.5, pd::EWMAlphaType::CenterOfMass, false);

    REQUIRE(result.size() == 5);
    INFO(result);
    REQUIRE(result.at(0).as<double>() == Catch::Approx(1));
    REQUIRE(result.at(1).as<double>() == Catch::Approx(1.666667).epsilon(0.01));
    REQUIRE(result.at(2).as<double>() == Catch::Approx(2.555556).epsilon(0.01));
    REQUIRE(result.at(3).as<double>() == Catch::Approx(3.518519).epsilon(0.01));
    REQUIRE(result.at(4).as<double>() == Catch::Approx(4.506173).epsilon(0.01));

    result = Series(std::vector<double>{ 2.0, 3.0, NAN, 5.0, 6.0 })
                 .ewm(pd::EWMAgg::Mean, 0.5, pd::EWMAlphaType::CenterOfMass, false, true);

    REQUIRE(result.size() == 5);
    INFO(result);
    REQUIRE(result.at(0).as<double>() == Catch::Approx(2.000000));
    REQUIRE(result.at(1).as<double>() == Catch::Approx(2.666667).epsilon(0.01));
    REQUIRE(result.at(2).as<double>() == Catch::Approx(2.666667).epsilon(0.01));
    REQUIRE(result.at(3).as<double>() == Catch::Approx(4.222222).epsilon(0.01));
    REQUIRE(result.at(4).as<double>() == Catch::Approx(5.407407).epsilon(0.01));

    result = Series(std::vector<double>{ 2.0, 3.0, NAN, 5.0, 6.0 })
                 .ewm(pd::EWMAgg::Mean, 0.5, pd::EWMAlphaType::CenterOfMass, true, true);

    REQUIRE(result.size() == 5);
    INFO(result);
    REQUIRE(result.at(0).as<double>() == Catch::Approx(2.000000));
    REQUIRE(result.at(1).as<double>() == Catch::Approx(2.750000).epsilon(0.01));
    REQUIRE(result.at(2).as<double>() == Catch::Approx(2.750000).epsilon(0.01));
    REQUIRE(result.at(3).as<double>() == Catch::Approx(4.307692).epsilon(0.01));
    REQUIRE(result.at(4).as<double>() == Catch::Approx(5.450000).epsilon(0.01));
}

TEST_CASE("Test reindex vs reindex_async benchmark small data", "[.reindex]")
{
    // Create a test input Series
    auto inputData = pd::random::RandomState(100).randn(100, 0, 2);
    auto inputIndex = pd::date_range(ptime(), 100, "T");
    pd::Series inputSeries(inputData, "", inputIndex);

    auto newIndex = pd::date_range(ptime(), 100, "30S");
    pd::Series sortedOut{ nullptr, nullptr }, out{ nullptr, nullptr };

    BENCHMARK("benchmark reindex")
    {
        sortedOut = inputSeries.reindex(newIndex);
        return 1;
    };

    BENCHMARK("benchmark reindex_async")
    {
        out = inputSeries.reindexAsync(newIndex);
        return 1;
    };

    REQUIRE(sortedOut.array()->Equals(out.array()));
    REQUIRE(sortedOut.indexArray()->Equals(out.indexArray()));
}

TEST_CASE("Test reindex vs reindex_async benchmark avg case", "[.reindex]")
{
    // Create a test input Series
    auto inputData = pd::random::RandomState(100).randn(1e6, 0, 2);
    auto inputIndex = pd::date_range(ptime(), 1e5, "T");
    pd::Series inputSeries(inputData, "", inputIndex);

    auto newIndex = pd::date_range(ptime(), 1e6, "30S");
    pd::Series sortedOut{ nullptr, nullptr }, out{ nullptr, nullptr };

    auto start = std::chrono::high_resolution_clock::now();
    sortedOut = inputSeries.reindex(newIndex);
    auto end = std::chrono::high_resolution_clock::now();

    std::cout << std::chrono::duration<double>(end - start).count() << " s.\n";

    start = std::chrono::high_resolution_clock::now();
    out = inputSeries.reindexAsync(newIndex);
    end = std::chrono::high_resolution_clock::now();

    std::cout << std::chrono::duration<double>(end - start).count() << " s.\n";
}

TEST_CASE("Test reindex vs reindex_async benchamark big data", "[.reindex]")
{
    // Create a test input Series
    auto inputData = pd::random::RandomState(100).randn(5e6, 0, 2);
    auto inputIndex = pd::date_range(ptime(), 5e6);
    pd::Series inputSeries(inputData, "", inputIndex);

    auto newIndex = pd::date_range(ptime() + minutes(1), 1e6);
    pd::Series sortedOut{ nullptr, nullptr }, out{ nullptr, nullptr };

    auto start = std::chrono::high_resolution_clock::now();
    sortedOut = inputSeries.reindex(newIndex);
    auto end = std::chrono::high_resolution_clock::now();

    std::cout << std::chrono::duration<double>(end - start).count() << " s.\n";

    start = std::chrono::high_resolution_clock::now();
    out = inputSeries.reindexAsync(newIndex);
    end = std::chrono::high_resolution_clock::now();

    std::cout << std::chrono::duration<double>(end - start).count() << " s.\n";
}

TEST_CASE("Test reindex function", "[reindex]")
{
    // Create a test input Series
    auto inputData = arrow::ArrayT<::int64_t>::Make({ 1, 2, 3, 4, 5 });
    auto inputIndex = arrow::ArrayT<::int64_t>::Make({ 1, 2, 3, 4, 5 });
    pd::Series inputSeries(inputData, inputIndex);

    // Create a new index array
    auto newIndex = arrow::ArrayT<::int64_t>::Make({ 1, 2, 4, 5, 6 });

    // Reindex the input Series
    pd::Series outputSeries = inputSeries.reindex(newIndex);

    // Check that the new index is correct
    REQUIRE(outputSeries.indexArray()->Equals(newIndex));

    // Check that the values are correct
    auto expectedValues = arrow::ArrayT<::int64_t>::Make({ 1, 2, 4, 5, 0 }, { true, true, 1, 1, 0 });
    REQUIRE(outputSeries.array()->Equals(expectedValues));
}

TEST_CASE("Test toFrame", "[to_frame]")
{
    auto series = pd::Series{ std::vector<std::string>{ "a", "b", "c" }, "vals" };

    auto df = series.toFrame();

    auto expected = pd::DataFrame{ pd::ArrayPtr{ nullptr }, std::pair{ "vals"s, std::vector{ "a"s, "b"s, "c"s } } };

    REQUIRE(df.equals_(expected));

    df = series.toFrame("val");

    expected = pd::DataFrame{ pd::ArrayPtr{ nullptr }, std::pair{ "val"s, std::vector{ "a"s, "b"s, "c"s } } };

    REQUIRE(df.equals_(expected));
}

TEST_CASE("Test GetSpan") {
    auto series = pd::Series{ std::vector<int64_t>{ 1, 2, 3, 4, 5 }, "vals" };
    REQUIRE(series.getSpan<int64_t>().front() == 1);
    REQUIRE(series.getSpan<int64_t>().back() == 5);
    REQUIRE(series.getIndexSpan<uint64_t>().front() == 0);
    REQUIRE(series.getIndexSpan<uint64_t>().back() == 4);

    series = pd::Series{ std::vector<std::string>{ "1", "2", "3" }, "vals", arrow::ArrayT<std::string>::Make(std::vector<std::string>{"jane", "hello", "hi"}) };
    REQUIRE_THROWS_AS(series.getSpan<int64_t>(), std::runtime_error);
    REQUIRE_THROWS_AS(series.getIndexSpan<int64_t>(), std::runtime_error);
}