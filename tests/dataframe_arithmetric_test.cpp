//
// Created by adesola on 1/11/25.
//
#include "catch.hpp"
#include "pandas_arrow.h"


using namespace pd;          // or qualify as needed: pd::DataFrame, pd::Series, etc.
using Catch::Approx;

TEST_CASE("Arithmetic operations on DataFrame", "[DataFrame][arithmetic]") 
{
    SECTION("Setup: Construct small DataFrames")
    {
        // We'll create two 3x2 DataFrames with integer columns.

        // df1:
        //   colA   colB
        //0     1      4
        //1     2      5
        //2     3      6
        DataFrame df1(
                std::map<std::string, std::vector<int>>{
                        {"colA", {1, 2, 3}},
                        {"colB", {4, 5, 6}}
                }
        );

        // df2:
        //   colA   colB
        //0     10    -1
        //1     20    -2
        //2     30    -3
        DataFrame df2(
                std::map<std::string, std::vector<double>>{
                        {"colA", {10., 20., 30.}},
                        {"colB", {-1., -2., -3.}}
                }
        );

        // Create a DataFrame for checking shape mismatch
        DataFrame df3(
                std::map<std::string, std::vector<int>>{
                        {"colX", {1, 2}},   // only 2 rows
                        {"colY", {3, 4}}
                }
        );

        SECTION("DataFrame + DataFrame") 
        {
            DataFrame result = df1 + df2;
            // We expect:
            // colA: [1+10, 2+20, 3+30] => [11, 22, 33]
            // colB: [4+(-1), 5+(-2), 6+(-3)] => [3, 3, 3]

            // You might retrieve columns as Series then convert to std::vector.
            auto colAResult = result["colA"].values<double>();
            auto colBResult = result["colB"].values<double>();

            REQUIRE(colAResult.size() == 3);
            REQUIRE(colBResult.size() == 3);

            REQUIRE(colAResult[0] == 11);
            REQUIRE(colAResult[1] == 22);
            REQUIRE(colAResult[2] == 33);

            REQUIRE(colBResult[0] == 3);
            REQUIRE(colBResult[1] == 3);
            REQUIRE(colBResult[2] == 3);

            // Attempt a shape mismatch
            REQUIRE_THROWS_AS(df1 + df3, std::runtime_error);
        }

        SECTION("DataFrame - DataFrame") 
        {
            DataFrame result = df1 - df2;
            // We expect:
            // colA: [1-10, 2-20, 3-30] => [-9, -18, -27]
            // colB: [4-(-1), 5-(-2), 6-(-3)] => [5, 7, 9]

            auto colAResult = result["colA"].values<double>();
            auto colBResult = result["colB"].values<double>();

            REQUIRE(colAResult.size() == 3);
            REQUIRE(colBResult.size() == 3);

            REQUIRE(colAResult[0] == -9);
            REQUIRE(colAResult[1] == -18);
            REQUIRE(colAResult[2] == -27);

            REQUIRE(colBResult[0] == 5);
            REQUIRE(colBResult[1] == 7);
            REQUIRE(colBResult[2] == 9);

            // Attempt a shape mismatch
            REQUIRE_THROWS_AS(df2 - df3, std::runtime_error);
        }

        SECTION("DataFrame * DataFrame")
        {
            DataFrame result = df1 * df2;
            // We expect:
            // colA: [1*10, 2*20, 3*30] => [10, 40, 90]
            // colB: [4*(-1), 5*(-2), 6*(-3)] => [-4, -10, -18]

            auto colAResult = result["colA"].values<double>();
            auto colBResult = result["colB"].values<double>();

            REQUIRE(colAResult.size() == 3);
            REQUIRE(colBResult.size() == 3);

            REQUIRE(colAResult[0] == 10);
            REQUIRE(colAResult[1] == 40);
            REQUIRE(colAResult[2] == 90);

            REQUIRE(colBResult[0] == -4);
            REQUIRE(colBResult[1] == -10);
            REQUIRE(colBResult[2] == -18);

            // Attempt a shape mismatch
            REQUIRE_THROWS_AS(df2 * df3, std::runtime_error);
        }

        SECTION("DataFrame / DataFrame")
        {
            // Try dividing df1 by df2
            // colA => [1/10, 2/20, 3/30] => [0.1, 0.1, 0.1] (assuming float or double)
            // colB => [4/(-1), 5/(-2), 6/(-3)] => [-4, -2.5, -2]
            DataFrame result = df1 / df2;

            // We'll assume arrow is storing floats/doubles. So let's do approximate checks:
            auto colAResult = result["colA"].values<double>();
            auto colBResult = result["colB"].values<double>();

            REQUIRE(colAResult.size() == 3);
            REQUIRE(colBResult.size() == 3);

            REQUIRE(colAResult[0] == Approx(0.1));
            REQUIRE(colAResult[1] == Approx(0.1));
            REQUIRE(colAResult[2] == Approx(0.1));

            REQUIRE(colBResult[0] == Approx(-4.0));
            REQUIRE(colBResult[1] == Approx(-2.5));
            REQUIRE(colBResult[2] == Approx(-2.0));

            // Attempt a shape mismatch
            REQUIRE_THROWS_AS(df1 / df3, std::runtime_error);
        }

        SECTION("DataFrame operator+(Series)")
        {
            // We'll add a Series to df1: seriesS = [100, 200, 300] as "column broadcast"
            Series seriesS(std::vector<double>{100, 200, 300});
            DataFrame result = df1 + seriesS;
            // Expect:
            //   colA => [101, 202, 303]
            //   colB => [104, 205, 306]

            auto colAResult = result["colA"].values<double>();
            auto colBResult = result["colB"].values<double>();

            REQUIRE(colAResult[0] == 101);
            REQUIRE(colAResult[1] == 202);
            REQUIRE(colAResult[2] == 303);

            REQUIRE(colBResult[0] == 104);
            REQUIRE(colBResult[1] == 205);
            REQUIRE(colBResult[2] == 306);

            // If shape mismatch in the Series -> exception
            Series shortSeries(std::vector<int>{1, 2}); 
            REQUIRE_THROWS_AS(df1 + shortSeries, std::runtime_error);
        }

        SECTION("DataFrame operator+(Scalar)")
        {
            // We'll add a Scalar of 10
            Scalar sc(10);
            DataFrame result = df1 + sc;
            // Expect:
            //   colA => [11, 12, 13]
            //   colB => [14, 15, 16]

            auto colAResult = result["colA"].values<int>();
            auto colBResult = result["colB"].values<int>();

            REQUIRE(colAResult[0] == 11);
            REQUIRE(colAResult[1] == 12);
            REQUIRE(colAResult[2] == 13);

            REQUIRE(colBResult[0] == 14);
            REQUIRE(colBResult[1] == 15);
            REQUIRE(colBResult[2] == 16);
        }

        SECTION("Unary operations: abs, exp, sqrt, sign, pow")
        {
            // Create a DataFrame with negative entries and zero for thorough checks
            DataFrame dfNeg(
                    std::map<std::string, std::vector<double>>{
                            {"colN", {-4.0, -1.0, 0.0, 2.0}},
                            {"colM", { 1.0,  4.0, 9.0, 16.0}}
                    }
            );

            SECTION("abs()")
            {
                DataFrame absed = dfNeg.abs();
                auto colN = absed["colN"].values<double>();
                auto colM = absed["colM"].values<double>();

                REQUIRE(colN == std::vector<double>{4.0, 1.0, 0.0, 2.0});
                REQUIRE(colM == std::vector<double>{1.0, 4.0, 9.0, 16.0});
            }

            SECTION("exp()")
            {
                DataFrame exped = dfNeg.exp();
                auto colN = exped["colN"].values<double>();
                auto colM = exped["colM"].values<double>();

                // Check a few approximate values
                // e^-4.0 ~ 0.0183156
                // e^-1.0 ~ 0.3678794
                // e^0.0  ~ 1.0
                // e^2.0  ~ 7.389056
                REQUIRE(colN[0] == Approx(0.0183156));
                REQUIRE(colN[1] == Approx(0.3678794));
                REQUIRE(colN[2] == Approx(1.0));
                REQUIRE(colN[3] == Approx(7.389056));

                // e^1.0  ~ 2.7182818
                // e^4.0  ~ 54.5981500
                // e^9.0  ~ 8103.0839276
                // e^16.0 ~ ~8.8861105e6
                // We'll just verify the first few to be safe:
                REQUIRE(colM[0] == Approx(2.71828).epsilon(0.0001));
                REQUIRE(colM[1] == Approx(54.59815).epsilon(0.0001));
            }

            SECTION("sqrt()")
            {
                // sqrt() of negative values => typically NaN
                DataFrame rooted = dfNeg.sqrt();
                auto colN = rooted["colN"].values<double>();
                auto colM = rooted["colM"].values<double>();

                // colN => sqrt(-4.0), sqrt(-1.0), sqrt(0.0), sqrt(2.0)
                // Expect: [NaN, NaN, 0.0, ~1.4142]
                REQUIRE(std::isnan(colN[0]));
                REQUIRE(std::isnan(colN[1]));
                REQUIRE(colN[2] == Approx(0.0));
                REQUIRE(colN[3] == Approx(1.4142).epsilon(0.001));

                // colM => sqrt(1.0), sqrt(4.0), sqrt(9.0), sqrt(16.0)
                // => [1.0, 2.0, 3.0, 4.0]
                REQUIRE(colM[0] == Approx(1.0));
                REQUIRE(colM[1] == Approx(2.0));
                REQUIRE(colM[2] == Approx(3.0));
                REQUIRE(colM[3] == Approx(4.0));
            }

            SECTION("sign()")
            {
                // sign() => -1 if negative, 0 if zero, +1 if positive
                DataFrame signedDF = dfNeg.sign();
                auto colN = signedDF["colN"].values<double>();
                auto colM = signedDF["colM"].values<double>();

                // colN => [-4.0, -1.0, 0.0, 2.0] => [-1, -1, 0, +1]
                REQUIRE(colN[0] == -1);
                REQUIRE(colN[1] == -1);
                REQUIRE(colN[2] == 0);
                REQUIRE(colN[3] == 1);

                // colM => [1.0, 4.0, 9.0, 16.0] => [1, 1, 1, 1]
                REQUIRE(colM[0] == 1);
                REQUIRE(colM[1] == 1);
                REQUIRE(colM[2] == 1);
                REQUIRE(colM[3] == 1);
            }

            SECTION("pow(x)")
            {
                // Raise each element to the power 2.0
                DataFrame powered = dfNeg.pow(2.0);
                auto colN = powered["colN"].values<double>();
                auto colM = powered["colM"].values<double>();

                // colN => [-4.0, -1.0, 0.0, 2.0] => [16.0, 1.0, 0.0, 4.0]
                REQUIRE(colN[0] == Approx(16.0));
                REQUIRE(colN[1] == Approx(1.0));
                REQUIRE(colN[2] == Approx(0.0));
                REQUIRE(colN[3] == Approx(4.0));

                // colM => [1.0, 4.0, 9.0, 16.0] => [1.0, 16.0, 81.0, 256.0]
                REQUIRE(colM[0] == Approx(1.0));
                REQUIRE(colM[1] == Approx(16.0));
                REQUIRE(colM[2] == Approx(81.0));
                REQUIRE(colM[3] == Approx(256.0));
            }
        }
    }
}