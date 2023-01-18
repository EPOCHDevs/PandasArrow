#include "arrow/testing/gtest_util.h"
#include "arrow/testing/random.h"
#include "arrow/util/decimal.h"
#include "arrow/util/int128_internal.h"
#include "catch.hpp"
#include "pandas_arrow/pandas_arrow.h"
#include "vector"


using namespace arrow;
using namespace arrow::compute;
using namespace arrow::compute::internal;

TEST_CASE("SumArray2WithCovariance", "[covariance]")
{
    SECTION("two arrays with identical values")
    {
        std::shared_ptr<arrow::Array> x =
            arrow::ArrayT<double>::Make({ 1.0, 2.0, 3.0 });
        std::shared_ptr<arrow::Array> y =
            arrow::ArrayT<double>::Make({ 1.0, 2.0, 3.0 });

        arrow::ArraySpan data1(*x->data());
        arrow::ArraySpan data2(*y->data());

        REQUIRE(
            SumArray2WithCovariance<double, double, SimdLevel::NONE>(
                data1,
                data2,
                [](double a, double b) { return a * b; }) == 14.0);
    }

    SECTION("two arrays with different values")
    {
        std::shared_ptr<arrow::Array> x =
            arrow::ArrayT<double>::Make({ 1.0, 2.0, 3.0 });
        std::shared_ptr<arrow::Array> y =
            arrow::ArrayT<double>::Make({ 4.0, 5.0, 6.0});

        arrow::ArraySpan data1(*x->data());
        arrow::ArraySpan data2(*y->data());

        REQUIRE(
            SumArray2WithCovariance<double, double, SimdLevel::NONE>(
                data1,
                data2,
                [](double a, double b) { return a * b; }) == 32.0);
    }

    SECTION("two arrays with null values")
    {
        std::shared_ptr<arrow::DoubleArray> x,y;
        std::vector<double> data1 = {1.0, 2.0, std::numeric_limits<double>::quiet_NaN(), 4.0};
        std::vector<double> data2 = {4.0, std::numeric_limits<double>::quiet_NaN(), 6.0, 8.0};
        arrow::DoubleBuilder builder1,builder2;
        builder1.AppendValues(data1, {true, true, false, true});
        builder1.Finish(&x);
        builder2.AppendValues(data2, {true, false, true, true});
        builder2.Finish(&y);
        REQUIRE(
            std::isnan(SumArray2WithCovariance<double, double, SimdLevel::NONE>(
                *x->data(),
                *y->data(),
                [](double a, double b) {return a*b;} )));
    }
}

TEST_CASE("CovarianceImpl", "[covariance]") {
    // Create two arrays for testing
    std::shared_ptr<arrow::Array> x = arrow::ArrayT<double>::Make({1.0, 2.0, 3.0, 4.0});
    std::shared_ptr<arrow::Array> y = arrow::ArrayT<double>::Make({2.0, 3.0, 4.0, 5.0});

    // create an instance of CovarianceImpl
    VarianceOptions opt{1};
    arrow::compute::internal::CovarianceImpl<DoubleType> impl(0, arrow::float64(), opt);

    // Test Consume function
    std::vector<ExecValue> values{*x->data(), *y->data()};
    ExecSpan batch(values, values.size());
    REQUIRE(impl.Consume(nullptr, batch).ok());

    SECTION("Covariance of Batch1 "){
        Datum out;
        REQUIRE(impl.Finalize(nullptr, &out).ok());
        auto final_result = checked_pointer_cast<DoubleScalar>(out.scalar());
        REQUIRE(final_result->value == Catch::Approx(1.6667).epsilon(0.0001));
    }

    // Test MergeFrom function
    std::shared_ptr<arrow::Array> x2 = arrow::ArrayT<double>::Make({2.0, 3.0, 4.0, 5.0});
    std::shared_ptr<arrow::Array> y2 = arrow::ArrayT<double>::Make({3.0, 4.0, 5.0, 6.0});
    arrow::compute::internal::CovarianceImpl<DoubleType> impl2(0, arrow::float64(), opt);

    std::vector<ExecValue> values2{*x2->data(), *y2->data()};
    ExecSpan batch2(values2, values2.size());

    SECTION("Covariance of Merged")
    {
        // Test MergeFrom function
        std::shared_ptr<arrow::Array> x = arrow::ArrayT<double>::Make({1.0, 2.0, 3.0, 4.0, 2.0, 3.0, 4.0, 5.0});
        std::shared_ptr<arrow::Array> y = arrow::ArrayT<double>::Make({2.0, 3.0, 4.0, 5.0, 3.0, 4.0, 5.0, 6.0});
        arrow::compute::internal::CovarianceImpl<DoubleType> imp(0, arrow::float64(), opt);

        std::vector<ExecValue> v{*x->data(), *y->data()};
        ExecSpan b(v, v.size());
        REQUIRE(imp.Consume(nullptr, b).ok());

        Datum out;
        REQUIRE(imp.Finalize(nullptr, &out).ok());
        auto final_result = checked_pointer_cast<DoubleScalar>(out.scalar());
        REQUIRE(final_result->value == Catch::Approx(1.7142857143).epsilon(0.0001));
    }


    REQUIRE(impl2.Consume(nullptr, batch2).ok());
    SECTION("Covariance of Batch2 "){
        Datum out;
        REQUIRE(impl2.Finalize(nullptr, &out).ok());
        auto final_result = checked_pointer_cast<DoubleScalar>(out.scalar());
        REQUIRE(final_result->value == Catch::Approx(1.6667).epsilon(0.0001));
    }

    REQUIRE(impl2.Consume(nullptr, batch2).ok());
    SECTION("Covariance of Merged "){
        REQUIRE(impl.MergeFrom(nullptr,  std::move(impl2)).ok());

        // Test Finalize function
        Datum out;
        REQUIRE(impl.Finalize(nullptr, &out).ok());
        auto final_result = checked_pointer_cast<DoubleScalar>(out.scalar());
        REQUIRE(final_result->value == Catch::Approx( 1.7142857143));
    }
}

TEST_CASE("CorrelationImpl", "[correlation]") {
    // Create two arrays for testing
    std::shared_ptr<arrow::Array> x = arrow::ArrayT<double>::Make({1.0, 2.0, 3.0, 4.0});
    std::shared_ptr<arrow::Array> y = arrow::ArrayT<double>::Make({2.0, 3.0, 4.0, 5.0});

    // create an instance of CovarianceImpl
    VarianceOptions opt{1};
    arrow::compute::internal::CorrelationImpl<DoubleType> impl(0, arrow::float64(), opt);

    // Test Consume function
    std::vector<ExecValue> values{*x->data(), *y->data()};
    ExecSpan batch(values, values.size());
    REQUIRE(impl.Consume(nullptr, batch).ok());

    SECTION("Correlation of Batch1 "){
        Datum out;
        REQUIRE(impl.Finalize(nullptr, &out).ok());
        auto final_result = checked_pointer_cast<DoubleScalar>(out.scalar());
        REQUIRE(final_result->value == Catch::Approx(1).epsilon(0.0001));
    }

    // Test MergeFrom function
    std::shared_ptr<arrow::Array> x2 = arrow::ArrayT<double>::Make({2.0, 1.0, -4.0, -5.0});
    std::shared_ptr<arrow::Array> y2 = arrow::ArrayT<double>::Make({3.0, 4.0, 5.0, 6.0});
    arrow::compute::internal::CorrelationImpl<DoubleType> impl2(0, arrow::float64(), opt);

    std::vector<ExecValue> values2{*x2->data(), *y2->data()};
    ExecSpan batch2(values2, values2.size());

    SECTION("Correlation of Merged")
    {
        // Test MergeFrom function
        std::shared_ptr<arrow::Array> x3 = arrow::ArrayT<double>::Make({1.0, 2.0, 3.0, 4.0, 2.0, 1.0, -4.0, -5.0});
        std::shared_ptr<arrow::Array> y3 = arrow::ArrayT<double>::Make({2.0, 3.0, 4.0, 5.0, 3.0, 4.0, 5.0, 6.0});
        arrow::compute::internal::CorrelationImpl<DoubleType> imp(0, arrow::float64(), opt);

        std::vector<ExecValue> v{*x3->data(), *y3->data()};
        ExecSpan b(v, v.size());
        REQUIRE(imp.Consume(nullptr, b).ok());

        Datum out;
        REQUIRE(imp.Finalize(nullptr, &out).ok());
        auto final_result = checked_pointer_cast<DoubleScalar>(out.scalar());
        REQUIRE(final_result->value == Catch::Approx(-0.53692484).epsilon(0.0001));
    }

    REQUIRE(impl2.Consume(nullptr, batch2).ok());
    SECTION("Correlation of Batch2 "){
        Datum out;
        REQUIRE(impl2.Finalize(nullptr, &out).ok());
        auto final_result = checked_pointer_cast<DoubleScalar>(out.scalar());
        REQUIRE(final_result->value == Catch::Approx(-0.95577901).epsilon(0.0001));
    }

    REQUIRE(impl2.Consume(nullptr, batch2).ok());
    SECTION("Correlation of Merged "){
        REQUIRE(impl.MergeFrom(nullptr,  std::move(impl2)).ok());

        // Test Finalize function
        Datum out;
        REQUIRE(impl.Finalize(nullptr, &out).ok());
        auto final_result = checked_pointer_cast<DoubleScalar>(out.scalar());
        REQUIRE(final_result->value == Catch::Approx( -0.53692484));
    }
}

TEST_CASE("Test shift operation on int32 array", "[shift]") {
    std::vector<int32_t> x = {1, 2, 3, 4, 5};
    auto array = ArrayT<int32_t>::Make(x);
    auto maybe_shifted_array = Shift(array, ShiftOptions(-2, arrow::MakeScalar(0)));
    std::vector<int32_t> expected_result = {3, 4, 5, 0, 0};
    if(maybe_shifted_array.ok())
    {
        auto shifted_array  = maybe_shifted_array.MoveValueUnsafe().make_array();
        INFO(shifted_array->ToString() );
        REQUIRE(shifted_array->ApproxEquals(ArrayT<int32_t>::Make(expected_result)));
    }
    else
    {
        throw std::runtime_error(maybe_shifted_array.status().message());
    }
}

TEST_CASE("Test shift operation on float array", "[shift]") {
    std::vector<float> x = {1.1, 2.2, 3.3, 4.4, 5.5};
    auto array = ArrayT<float>::Make(x);
    auto maybe_shifted_array = Shift(array, ShiftOptions(-2, arrow::MakeScalar(0.f)));
    std::vector<float> expected_result = {3.3, 4.4, 5.5, 0.0, 0.0};

    if(maybe_shifted_array.ok())
    {
        auto shifted_array  = maybe_shifted_array.MoveValueUnsafe().make_array();
        INFO(shifted_array->ToString() );
        REQUIRE(shifted_array->ApproxEquals(ArrayT<float>::Make(expected_result)));
    }
    else
    {
        throw std::runtime_error(maybe_shifted_array.status().message());
    }

}

TEST_CASE("Test shift operation on string array", "[shift]") {
    std::vector<std::string> x = {"a", "b", "c", "d", "e"};
    auto array = ArrayT<std::string>::Make(x);
    auto maybe_shifted_array = Shift(array, ShiftOptions(2, arrow::MakeScalar("z")));
    std::vector<std::string> expected_result = {"z", "z", "a", "b", "c"};
    if(maybe_shifted_array.ok())
    {
        auto shifted_array = maybe_shifted_array.MoveValueUnsafe().make_array();
        INFO(shifted_array->ToString());
        REQUIRE(
            shifted_array->Equals(ArrayT<std::string>::Make(expected_result)));
    }
    else
    {
    throw std::runtime_error(maybe_shifted_array.status().message());
    }

}

TEST_CASE("Test shift operation on array with null values", "[shift]")
{
    std::vector<int32_t> x = { 1, 2, 3, 4, 5 };
    std::vector<bool> mask = { 1, 1, 0, 0, 1 };
    auto array = ArrayT<int32_t>::Make(x, mask);
    auto maybe_shifted_array =
        Shift(array, ShiftOptions(2, arrow::MakeScalar(5)));
    std::vector<int32_t> expected_result = { 5, 5, 1, 2, 1 };

    if (maybe_shifted_array.ok())
    {
        auto shifted_array = maybe_shifted_array.MoveValueUnsafe().make_array();
        INFO(shifted_array->ToString());
        REQUIRE(shifted_array->Equals(ArrayT<int32_t>::Make(expected_result,
                                                            { 1, 1, 1, 1, 0 })));
    }
    else
    {
        throw std::runtime_error(maybe_shifted_array.status().message());
    }
}

TEST_CASE("Test pctchange operation on float array", "[pctchange]")
{
    std::vector<float> x = { 1.1, 2.2, 3.3, 4.4, 5.5 };
    auto array = ArrayT<float>::Make(x);
    auto maybe_pctchange_array = PctChange(array, 2);
    std::vector<double> expected_result = { 0,
                                           0,
                                           2,
                                           1,
                                           0.6666667461395264 };
    if (maybe_pctchange_array.ok())
    {
    auto pctchange_array = maybe_pctchange_array.ValueOrDie().make_array();
    INFO(pctchange_array->ToString());

    auto result = ArrayT<double>::Make(
        expected_result,
        { false, false, true, true, true });

    REQUIRE(pctchange_array->ApproxEquals(result));
    }
    else
    {
    throw std::runtime_error(maybe_pctchange_array.status().message());
    }
}

//TEST_CASE("Test autocorr operation on array", "[autocorr]")
//{
//    std::vector<double> x = { 0.25, 0.5, 0.2, -0.05 };
//    auto array = ArrayT<double>::Make(x);
//    auto maybe_scalar = AutoCorr(array);
//    if (maybe_scalar.ok())
//    {
//    auto maybe_expected =
//        maybe_scalar.MoveValueUnsafe().scalar_as<DoubleScalar>();
//    REQUIRE(maybe_expected.value == Catch::Approx(0.10355));
//    }
//    else
//    {
//    throw std::runtime_error(maybe_scalar.status().message());
//    }
//}