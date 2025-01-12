//
// Created by adesola on 1/11/25.
//
#include "pandas_arrow.h"
#include "catch.hpp"

using namespace pd;
using namespace std;

TEST_CASE("Dataframe Concat")
{
    using namespace std::string_literals;
    auto na = std::nan("");
    pd::DataFrame df1{ ArrayPtr{ nullptr },
                       pair{ "letter"s, std::vector<std::string>{ "a", "b" } },
                       pair{ "number"s, std::vector{ 1L, 2L } } };

    pd::DataFrame df2{ ArrayPtr{ nullptr },
                       pair{ "letter"s, std::vector<std::string>{ "c", "d" } },
                       pair{ "number"s, std::vector{ 3L, 4L } } };

    pd::DataFrame df3{ ArrayPtr{ nullptr },
                       pair{ "letter"s, std::vector<std::string>{ "c", "d" } },
                       pair{ "number"s, std::vector{ 3L, 4L } },
                       pair{ "animal_weight"s, std::vector{ 100.1, 100.2 } } };

    pd::DataFrame df4{ ArrayPtr{ nullptr },
                       pair{ "animal_weight"s, std::vector{ 200.1, 200.2  } },
                       pair{ "name"s, std::vector{ "monkey"s, "george"s } } };

    DataFrame expected_result{ nullptr }, result{ nullptr };

    SECTION(
            "Combine DataFrame objects with overlapping columns and "
            "return everything. Columns outside the intersection "
            "will be filled with NaN values.",
            "[resample]")
    {

        expected_result = pd::DataFrame{ arrow::ArrayFromJSON<::uint64_t>("[0,1,0,1]"),
                                         pair{ "letter"s, std::vector<std::string>{ "a", "b", "c", "d" } },
                                         pair{ "number"s, std::vector<long>{ 1L, 2L, 3L, 4L } },
                                         pair{ "animal_weight"s, std::vector{ na, na, 100.1, 100.2 } } };

        result = pd::concat(std::vector{ df1, df3 }, AxisType::Index);
        // Assert that the resulting DataFrame is equal to the expected result
        INFO(result << "\n != \n" << expected_result);
        REQUIRE(result.equals_(expected_result));
    }

    SECTION("Combine DataFrame objects horizontally along the x axis", "[resample]")
    {
        expected_result = pd::DataFrame{ arrow::ArrayFromJSON<::uint64_t>("[0,1]"),
                                         pair{ "letter"s, std::vector<std::string>{ "a", "b" } },
                                         pair{ "number"s, std::vector<long>{ 1L, 2L } },
                                         pair{ "animal_weight"s, std::vector{ 200.1, 200.2  } },
                                         pair{ "name"s, std::vector{ "monkey"s, "george"s } } };

        result = pd::concat(std::vector{ df1, df4 }, AxisType::Columns);
        // Assert that the resulting DataFrame is equal to the expected result
        INFO(result << "\n != \n" << expected_result);
        REQUIRE(result.equals_(expected_result));
    }

    SECTION(
            "Combine DataFrame objects with overlapping columns and return"
            " only those that are shared by passing "
            "inner to the join keyword argument.",
            "[resample]")
    {
        expected_result = pd::DataFrame{ arrow::ArrayFromJSON<::uint64_t>("[0,1,0,1]"),
                                         pair{ "letter"s, std::vector<std::string>{ "a", "b", "c", "d" } },
                                         pair{ "number"s, std::vector{ 1L, 2L, 3L, 4L } } };

        result = pd::concat(std::vector{ df1, df3 }, AxisType::Index, JoinType::Inner);
        // Assert that the resulting DataFrame is equal to the expected result
        INFO(result << "\n != \n" << expected_result);
        REQUIRE(result.equals_(expected_result));
    }

    SECTION("Combine two DataFrame objects with identical columns", "[resample]")
    {
        expected_result = pd::DataFrame{ arrow::ArrayFromJSON<::uint64_t>("[0,1,0,1]"),
                                         pair{ "letter"s, std::vector<std::string>{ "a", "b", "c", "d" } },
                                         pair{ "number"s, std::vector{ 1L, 2L, 3L, 4L } } };

        result = pd::concat(std::vector{ df1, df2 }, AxisType::Index);
        // Assert that the resulting DataFrame is equal to the expected result
        INFO(result << "\n != \n" << expected_result);
        REQUIRE(result.equals_(expected_result));
    }

    SECTION("Combine two DataFrame objects with identical columns, IGNORE_INDEX", "[resample]")
    {
        auto d7 = pd::DataFrame{ arrow::ArrayFromJSON<::uint64_t>("[0]"),
                                 std::pair{ "a"s, std::vector{ 1L } },
                                 std::pair{ "b"s, std::vector{ 2L } } };

        auto d8 = pd::DataFrame{ arrow::ArrayFromJSON<::uint64_t>("[0]"),
                                 std::pair{ "a"s, std::vector{ 3L } },
                                 std::pair{ "b"s, std::vector{ 4L } } };

        expected_result = pd::DataFrame{ arrow::ArrayFromJSON<::uint64_t>("[0,1]"),
                                         pair{ "a"s, std::vector{ 1L, 3L } },
                                         pair{ "b"s, std::vector{ 2L, 4L } } };

        result = pd::concat(std::vector{ d7, d8 }, AxisType::Index, pd::JoinType::Outer, true);

        // Assert that the resulting DataFrame is equal to the expected result
        INFO(result << "\n != \n" << expected_result);
        REQUIRE(result.equals_(expected_result));
    }
}

TEST_CASE("ConcatenateColumns", "[concatenateColumns]")
{
    auto df1 = pd::DataFrame(pd::NULL_INDEX, pair{ "a"s, vector{ 1L, 2L, 3L } }, pair{ "b"s, vector{ 4L, 5L, 6L } });

    SECTION("Concatenate DataFrames with no duplicate keys")
    {
        auto df1 =
                pd::DataFrame(pd::NULL_INDEX, pair{ "a"s, vector{ 1L, 2L, 3L } }, pair{ "b"s, vector{ 4L, 5L, 6L } });

        auto df2 =
                pd::DataFrame(pd::NULL_INDEX, pair{ "c"s, vector{ 7L, 8L, 9L } }, pair{ "d"s, vector{ 10L, 11L, 12L } });

        auto result = pd::concat(std::vector{ df1, df2 }, AxisType::Columns);
        auto expected = pd::DataFrame(
                ArrayTable{ { "a", arrow::ArrayFromJSON<int64_t>("[1,2,3]") },
                            { "b", arrow::ArrayFromJSON<int64_t>("[4,5,6]") },
                            { "c", arrow::ArrayFromJSON<int64_t>("[7,8,9]") },
                            { "d", arrow::ArrayFromJSON<int64_t>("[10,11,12]") } },
                arrow::ArrayFromJSON<::uint64_t>("[0,1,2]"));

        REQUIRE(result.equals_(expected));
    }

    SECTION("Concatenate two DataFrames with duplicate keys")
    {
        auto df1 =
                pd::DataFrame(pd::NULL_INDEX, pair{ "a"s, vector{ 1L, 2L, 3L } }, pair{ "b"s, vector{ 4L, 5L, 6L } });
        auto df2 =
                pd::DataFrame(pd::NULL_INDEX, pair{ "a"s, vector{ 7L, 8L, 9L } }, pair{ "b"s, vector{ 10L, 11L, 12L } });

        auto result = pd::concat(std::vector<pd::DataFrame>{ df1, df2 }, AxisType::Columns);
        auto expected = pd::DataFrame(
                std::vector<std::vector<long>>{ { { 1L, 2L, 3L }, { 4L, 5L, 6L }, { 7, 8, 9 }, { 10, 11, 12 } } },
                { "a", "b", "a", "b" });

        INFO(result << "\n!=\n" << expected);
        REQUIRE(result.equals_(expected));
    }

    SECTION("Concatenate two DataFrames with different types of duplicate keys")
    {
        auto df1 =
                pd::DataFrame(pd::NULL_INDEX, pair{ "a"s, vector{ 1L, 2L, 3L } }, pair{ "b"s, vector{ 4L, 5L, 6L } });
        auto df2 = pd::DataFrame(
                pd::NULL_INDEX,
                pair{ "a"s, vector<double>{ 7.0, 8.0, 9.0 } },
                pair{ "b"s, vector{ 10L, 11L, 12L } });

        auto result = pd::concat(std::vector<pd::DataFrame>{ df1, df2 }, AxisType::Columns);
        auto expected = pd::DataFrame(
                std::array{ "a"s, "b"s, "a"s, "b"s },
                NULL_INDEX,
                std::vector{ 1L, 2L, 3L },
                std::vector{ 4L, 5L, 6L },
                std::vector{ 7.0, 8.0, 9.0 },
                std::vector{ 10L, 11L, 12L });

        INFO(result << "\n!=\n" << expected);
        REQUIRE(result.equals_(expected));
    }

    SECTION("Concatenate two DataFrames with duplicate keys and ignore_index = true")
    {
        auto df1 =
                pd::DataFrame(pd::NULL_INDEX, pair{ "a"s, vector{ 1L, 2L, 3L } }, pair{ "b"s, vector{ 4L, 5L, 6L } });
        auto df2 =
                pd::DataFrame(pd::NULL_INDEX, pair{ "a"s, vector{ 7L, 8L, 9L } }, pair{ "b"s, vector{ 10L, 11L, 12L } });

        auto result = pd::concat(std::vector<pd::DataFrame>{ df1, df2 }, AxisType::Columns, JoinType::Outer, true);
        auto expected = pd::DataFrame(
                std::vector<std::vector<long>>{ { { 1L, 2L, 3L }, { 4L, 5L, 6L }, { 7, 8, 9 }, { 10, 11, 12 } } },
                { "0", "1", "2", "3" });

        INFO(result << "\n!=\n" << expected);
        REQUIRE(result.equals_(expected));
    }

    SECTION("Concatenate two DataFrames with some duplicate keys and sort = true")
    {
        auto df1 =
                pd::DataFrame(pd::NULL_INDEX, pair{ "d"s, vector{ 1L, 2L, 3L } }, pair{ "e"s, vector{ 4L, 5L, 6L } });

        auto df2 =
                pd::DataFrame(pd::NULL_INDEX, pair{ "c"s, vector{ 7L, 8L, 9L } }, pair{ "e"s, vector{ 10L, 11L, 12L } });

        auto result =
                pd::concat(std::vector<pd::DataFrame>{ df1, df2 }, AxisType::Columns, JoinType::Outer, false, true);

        auto expected = pd::DataFrame(
                std::array<std::string, 4>{ "d", "e", "c", "e" },
                pd::ArrayPtr{ nullptr },
                std::vector{ 1L, 2L, 3L },
                std::vector{ 4L, 5L, 6L },
                std::vector{ 7L, 8L, 9L },
                std::vector{ 10L, 11L, 12L });

        INFO(result << "\n!=\n" << expected);
        REQUIRE(result.equals_(expected));
    }

    SECTION("Concatenate two DataFrames with some duplicate keys and sort = false")
    {
        auto df1 =
                pd::DataFrame(pd::NULL_INDEX, pair{ "d"s, vector{ 1L, 2L, 3L } }, pair{ "e"s, vector{ 4L, 5L, 6L } });

        auto df2 =
                pd::DataFrame(pd::NULL_INDEX, pair{ "c"s, vector{ 7L, 8L, 9L } }, pair{ "e"s, vector{ 10L, 11L, 12L } });

        auto result =
                pd::concat(std::vector<pd::DataFrame>{ df1, df2 }, AxisType::Columns, JoinType::Outer, false, false);

        auto expected = pd::DataFrame(
                std::array<std::string, 4>{ "d", "e", "c", "e" },
                pd::ArrayPtr{ nullptr },
                std::vector{ 1L, 2L, 3L },
                std::vector{ 4L, 5L, 6L },
                std::vector{ 7L, 8L, 9L },
                std::vector{ 10L, 11L, 12L });

        INFO(result << "\n!=\n" << expected);
        REQUIRE(result.equals_(expected));
    }

    SECTION("Concatenate DataFrames with different indexes")
    {
        auto df1 =
                pd::DataFrame(pd::NULL_INDEX, pair{ "a"s, vector{ 1L, 2L, 3L } }, pair{ "b"s, vector{ 4L, 5L, 6L } });

        auto df2 =
                pd::DataFrame(pd::NULL_INDEX, pair{ "a"s, vector{ 7L, 8L, 9L } }, pair{ "c"s, vector{ 10L, 11L, 12L } });

        auto result = pd::concat(std::vector<pd::DataFrame>{ df1, df2 }, AxisType::Columns);

        auto expected = pd::DataFrame(
                std::array<std::string, 4>{ "a", "b", "a", "c" },
                pd::ArrayPtr{ nullptr },
                std::vector{ 1L, 2L, 3L },
                std::vector{ 4L, 5L, 6L },
                std::vector{ 7L, 8L, 9L },
                std::vector{ 10L, 11L, 12L });

        INFO(result << "\n!=\n" << expected);
        REQUIRE(result.equals_(expected));
    }

    SECTION("Concatenate DataFrames with different indexes and ignore_index = true")
    {

        auto df2 =
                pd::DataFrame(pd::NULL_INDEX, pair{ "a"s, vector{ 7L, 8L, 9L } }, pair{ "c"s, vector{ 10L, 11L, 12L } });

        auto result = pd::concat(std::vector<pd::DataFrame>{ df1, df2 }, AxisType::Columns, JoinType::Outer, true);

        auto expected = pd::DataFrame(
                std::array<std::string, 4>{ "0", "1", "2", "3" },
                pd::ArrayPtr{ nullptr },
                std::vector{ 1L, 2L, 3L },
                std::vector{ 4L, 5L, 6L },
                std::vector{ 7L, 8L, 9L },
                std::vector{ 10L, 11L, 12L });

        INFO(result << "\n!=\n" << expected);
        REQUIRE(result.equals_(expected));
    }

    SECTION("Concatenate DataFrames with JoinType::Inner")
    {
        auto df2 =
                pd::DataFrame(pd::NULL_INDEX, pair{ "a"s, vector{ 7L, 8L, 9L } }, pair{ "b"s, vector{ 10L, 11L, 12L } });
        auto result = pd::concat(std::vector<pd::DataFrame>{ df1, df2 }, AxisType::Index, JoinType::Inner);

        auto expected = pd::DataFrame(
                ArrayTable{
                        { "a", arrow::ArrayFromJSON<int64_t>("[1,2,3,7,8,9]") },
                        { "b", arrow::ArrayFromJSON<int64_t>("[4,5,6,10,11,12]") },
                },
                arrow::ArrayFromJSON<::uint64_t>("[0,1,2,0,1,2]"));

        INFO(result << "\n!=\n" << expected);
        REQUIRE(result.equals_(expected));
    }
    SECTION("Concatenate DataFrames with different indexes and JoinType::Inner")
    {
        auto df2 =
                pd::DataFrame(pd::NULL_INDEX, pair{ "a"s, vector{ 7L, 8L, 9L } }, pair{ "c"s, vector{ 10L, 11L, 12L } });

        auto result = pd::concat(std::vector<pd::DataFrame>{ df1, df2 }, AxisType::Index, JoinType::Inner);

        auto expected = pd::DataFrame(
                ArrayTable{
                        { "a", arrow::ArrayFromJSON<int64_t>("[1,2,3,7,8,9]") },
                },
                arrow::ArrayFromJSON<::uint64_t>("[0,1,2,0,1,2]"));

        INFO(result << "\n!=\n" << expected);
        REQUIRE(result.equals_(expected));
    }
}


// TEST_CASE("Concatenate with Series", "[concat]")
//{
//    SECTION(
//        "Concatenating multiple Series with different indexes,"
//        " axis = 0, join = outer, ignore_index = false, sort = false")
//    {
//        auto s1 =
//            pd::Series(std::vector{ 1L, 2L, 3L }, "s1", pd::range(0UL, 3UL));
//        auto s2 =
//            pd::Series(std::vector{ 4L, 5L, 6L }, "", pd::range(3UL, 6UL));
//        auto s3 =
//            pd::Series(std::vector{ 7L, 8L, 9L }, "", pd::range(6UL, 9UL));
//        auto result = pd::concat(
//            std::vector<pd::Series>{ s1, s2, s3 },
//            AxisType::Index,
//            JoinType::Outer,
//            false,
//            false);
//        auto expected = pd::DataFrame(
//            std::array<std::string, 1>{ "" },
//            pd::ArrayPtr{ nullptr },
//            std::vector<long>{ 1, 2, 3, 4, 5, 6, 7, 8, 9 },
//            std::vector<::uint64_t>{ 0, 1, 2, 3, 4, 5, 6, 7, 8 });
//        INFO(result << "\n!=\n" << expected);
//        REQUIRE(result.equals_(expected));
//    }

//    SECTION(
//        "Concatenating multiple Series with different indexes, axis = 1, join = outer, ignore_index = true, sort =
//        true")
//    {
//        auto s1 =
//            pd::Series(std::vector{ 1L, 2L, 3L }, "s1", pd::range(0UL, 3UL));
//        auto s2 =
//            pd::Series(std::vector{ 4L, 5L, 6L }, "s2", pd::range(3UL, 6UL));
//        auto s3 =
//            pd::Series(std::vector{ 7L, 8L, 9L }, "s3", pd::range(6UL, 9UL));
//        auto result = pd::concat(
//            std::vector<pd::Series>{ s1, s2, s3 },
//            AxisType::Columns,
//            JoinType::Outer,
//            true,
//            true);
//        auto expected = pd::DataFrame(
//            ArrayTable{ { "s1",
//                          arrow::ArrayFromJSON<::int64_t>(
//                              "[1,2,3,null,null,null,null,null,null]") },
//                        { "s2",
//                          arrow::ArrayFromJSON<::int64_t>(
//                              "[null,null,null,4,5,6,null,null,null]") },
//                        { "s3",
//                          arrow::ArrayFromJSON<::int64_t>(
//                              "[null,null,null,null,null,null,7,8,9]") } });
//        INFO(result << "\n!=\n" << expected);
//        REQUIRE(result.equals_(expected));
//    }

//    SECTION(
//        "Concatenating multiple Series with different indexes, axis = 1, join = outer, ignore_index = true, sort =
//        true")
//    {
//        auto s1 =
//            pd::Series(std::vector{ 1L, 2L, 3L }, "s1", pd::range(0UL, 3UL));
//        auto s2 =
//            pd::Series(std::vector{ 4L, 5L, 6L }, "s2", pd::range(3UL, 6UL));
//        auto s3 =
//            pd::Series(std::vector{ 7L, 8L, 9L }, "s3", pd::range(6UL, 9UL));
//        auto result = pd::concat(
//            std::vector<pd::Series>{ s1, s2, s3 },
//            AxisType::Columns,
//            JoinType::Outer,
//            true,
//            true);
//        auto expected = pd::DataFrame(
//            ArrayTable{ { "s1",
//                          arrow::ArrayFromJSON<::int64_t>(
//                              "[1,2,3,null,null,null,null,null,null]") },
//                        { "s2",
//                          arrow::ArrayFromJSON<::int64_t>(
//                              "[null,null,null,4,5,6,null,null,null]") },
//                        { "s3",
//                          arrow::ArrayFromJSON<::int64_t>(
//                              "[null,null,null,null,null,null,7,8,9]") } });
//        INFO(result << "\n!=\n" << expected);
//        REQUIRE(result.equals_(expected));
//    }
//
//    SECTION(
//        "Concatenating multiple Series with duplicate keys,"
//        " axis = 0, join = inner, ignore_index = false, sort = false")
//    {
//        auto s1 =
//            pd::Series(std::vector{ 1L, 2L, 3L }, "a", pd::range(0UL, 3UL));
//        auto s2 =
//            pd::Series(std::vector{ 4L, 5L, 6L }, "b", pd::range(3UL, 6UL));
//        auto s3 =
//            pd::Series(std::vector{ 7L, 8L, 9L }, "a", pd::range(6UL, 9UL));
//        auto result = pd::concat(
//            std::vector<pd::Series>{ s1, s2, s3 },
//            AxisType::Index,
//            JoinType::Inner,
//            false,
//            false);
//        auto expected = pd::DataFrame(
//            std::array<std::string, 1>{ "" },
//            pd::ArrayPtr{ nullptr },
//            std::vector<long>{ 1, 2, 3, 4, 5, 6, 7, 8, 9 },
//            std::vector<::uint64_t>{ 0, 1, 2, 6, 7, 8 });
//        INFO(result << "\n!=\n" << expected);
//        REQUIRE(result.equals_(expected));
//    }
//
//    SECTION(
//        "Concatenating multiple Series with duplicate keys,"
//        " axis = 0, join = inner, ignore_index = false, sort = false")
//    {
//        auto s1 =
//            pd::Series(std::vector{ 1L, 2L, 3L }, "a", pd::range(0UL, 3UL));
//        auto s2 =
//            pd::Series(std::vector{ 4L, 5L, 6L }, "b", pd::range(3UL, 6UL));
//        auto s3 =
//            pd::Series(std::vector{ 7L, 8L, 9L }, "a", pd::range(6UL, 9UL));
//        auto result = pd::concat(
//            std::vector<pd::Series>{ s1, s2, s3 },
//            AxisType::Index,
//            JoinType::Inner,
//            false,
//            false);
//        auto expected = pd::DataFrame(
//            std::array<std::string, 1>{ "" },
//            pd::ArrayPtr{ nullptr },
//            std::vector<long>{ 1, 2, 3, 4, 5, 6, 7, 8, 9 });
//        INFO(result << "\n!=\n" << expected);
//        REQUIRE(result.equals_(expected));
//    }
//    SECTION(
//        "Concatenating multiple Series with duplicate keys,"
//        " axis = 0, join = inner, ignore_index = false, sort = true")
//    {
//        auto s1 =
//            pd::Series(std::vector{ 1L, 2L, 3L }, "a", pd::range(0UL, 3UL));
//        auto s2 =
//            pd::Series(std::vector{ 4L, 5L, 6L }, "b", pd::range(3UL, 6UL));
//        auto s3 =
//            pd::Series(std::vector{ 7L, 8L, 9L }, "a", pd::range(6UL, 9UL));
//        auto result = pd::concat(
//            std::vector<pd::Series>{ s1, s2, s3 },
//            AxisType::Index,
//            JoinType::Inner,
//            false,
//            true);
//        auto expected = pd::DataFrame(
//            std::array<std::string, 1>{ "a" },
//            pd::ArrayPtr{ nullptr },
//            std::vector<long>{ 1, 2, 3, 4, 5, 6, 7, 8, 9 });
//        INFO(result << "\n!=\n" << expected);
//        REQUIRE(result.equals_(expected));
//    }

//    SECTION("Concatenating a mix of DataFrames and Series with different indexes and types,"
//        " axis = 1, join = inner, ignore_index = false, sort = false") {
//        auto s1 = pd::Series(std::vector{1L, 2L, 3L}, "s1", pd::range(0UL, 3UL));
//        auto df1 = pd::DataFrame(
//            pd::NULL_INDEX,
//            pair{ "a"s, vector{ 4L, 5L, 6L } },
//            pair{ "b"s, vector{ 7L, 8L, 9L } });
//        auto s2 = pd::Series(std::vector{10L, 11L, 12L}, "s2", pd::range(3UL, 6UL));
//        auto df2 = pd::DataFrame(
//            pd::NULL_INDEX,
//            pair{ "c"s, vector{ 13L, 14L, 15L } },
//            pair{ "d"s, vector{ 16L, 17L, 18L } });
//        auto result = pd::concat(
//            std::vector<NDFrameType>{s1, df1, s2, df2},
//            AxisType::Columns,
//            JoinType::Inner,
//            false,
//            false);
//        auto expected = pd::DataFrame(
//            std::array<std::string, 4>{ "s1", "a", "b", "s2" },
//            pd::ArrayPtr{ nullptr },
//            std::vector{ 1, 2, 3 },
//            std::vector{ 4, 5, 6 },
//            std::vector{ 7, 8, 9 },
//            std::vector{ 10, 11, 12 }
//        );
//        INFO(result << "\n!=\n" << expected);
//        REQUIRE(result.equals_(expected));
//    }
// }

// TEST_CASE("Concatenate Benchmark", "[concat]")
//{
//    auto rand = pd::random::RandomState(100);
//    auto generateTable = [&rand](bool int_type, int N, ArrayTable& data)
//    {
//        for (int i = 0; i < N; i++)
//        {
//            data[std::to_string(rand.randint(1, 0, 50)[0])] = int_type ?
//                (ArrayPtr)arrow::ArrayT<::int64_t>::Make(
//                    rand.randint(1e6, 0, 100)) :
//                (ArrayPtr)arrow::ArrayT<double>::Make(rand.rand(1e6));
//        }
//    };
//
//    ArrayTable df1, df2, df3, df4, df5;
//
//    generateTable(true, 5, df1);
//    generateTable(false, 5, df1);
//    generateTable(true, 10, df1);
//
//    generateTable(true, 5, df2);
//    generateTable(true, 5, df2);
//    generateTable(true, 10, df2);
//
//    generateTable(false, 5, df3);
//    generateTable(false, 5, df3);
//    generateTable(false, 10, df3);
//
//    generateTable(true, 5, df4);
//    generateTable(false, 5, df4);
//    generateTable(false, 10, df4);
//
//    generateTable(false, 5, df5);
//    generateTable(false, 5, df5);
//    generateTable(true, 10, df5);
//
////    {
////        auto start = std::chrono::high_resolution_clock::now();
////
////        auto result = pd::concat(std::vector{ pd::DataFrame(df1),
////                                              pd::DataFrame(df2),
////                                              pd::DataFrame(df3),
////                                              pd::DataFrame(df4),
////                                              pd::DataFrame(df5) });
////
////        auto stop = std::chrono::high_resolution_clock::now();
////        auto duration = std::chrono::duration<double>(stop - start);
////
////        std::cout << "Index concat: took " << duration.count() << " seconds\nshape = ";
////        std::cout << result.shape()[0] << " " <<  result.shape()[1] << "]\n";
////    }
//    {
//        auto start = std::chrono::high_resolution_clock::now();
//
//        auto result = pd::concat(std::vector{ pd::DataFrame(df1, pd::range(0l, 1e6l)),
//                                              pd::DataFrame(df2, pd::range(10l, 1e6l+10l)),
//                                              pd::DataFrame(df3, pd::range(0l, 1e6l)),
//                                              pd::DataFrame(df4, pd::range(5l, 1e6l+5l)),
//                                              pd::DataFrame(df5, pd::range(0l, 1e6l)) },
//                                 AxisType::Columns);
//
//        auto stop = std::chrono::high_resolution_clock::now();
//        auto duration = std::chrono::duration<double>(stop - start);
//
//        std::cout << "Column concat: took " << duration.count() << " seconds\nshape = ";
//        std::cout << result.shape()[0] << " " <<  result.shape()[1] << "]\n";
//    }
//
////    {
////        auto start = std::chrono::high_resolution_clock::now();
////
////        auto result = pd::concatColumnsUnsafe(std::vector{ pd::DataFrame(df1, pd::range(0l, 1e6l)),
////                                              pd::DataFrame(df2, pd::range(10l, 1e6l+10l)),
////                                              pd::DataFrame(df3, pd::range(0l, 1e6l)),
////                                              pd::DataFrame(df4, pd::range(5l, 1e6l+5l)),
////                                              pd::DataFrame(df5, pd::range(0l, 1e6l)) });
////
////        auto stop = std::chrono::high_resolution_clock::now();
////        auto duration = std::chrono::duration<double>(stop - start);
////
////        std::cout << "Column unsafe concat: took " << duration.count() << " seconds\nshape = ";
////        std::cout << result.shape()[0] << " " <<  result.shape()[1] << "]\n";
////    }
//
//}

TEST_CASE("ConcatenateRows", "[concatenateRows]")
{

    auto df1 = pd::DataFrame(pd::NULL_INDEX, pair{ "a"s, vector{ 1L, 2L, 3L } }, pair{ "b"s, vector{ 4L, 5L, 6L } });

    SECTION("Concatenate two DataFrames with no duplicate keys")
    {
        auto df2 =
                pd::DataFrame(pd::NULL_INDEX, pair{ "c"s, vector{ 7L, 8L, 9L } }, pair{ "d"s, vector{ 10L, 11L, 12L } });

        auto concatenator = pd::Concatenator({ df1, df2 });

        auto result = concatenator(AxisType::Index);
        auto expected = pd::DataFrame(
                ArrayTable{ { "a", arrow::ArrayFromJSON<int64_t>("[1,2,3,null,null,null]") },
                            { "b", arrow::ArrayFromJSON<int64_t>("[4,5,6,null,null,null]") },
                            { "c", arrow::ArrayFromJSON<int64_t>("[null,null,null,7,8,9]") },
                            { "d", arrow::ArrayFromJSON<int64_t>("[null,null,null,10,11,12]") } },
                arrow::ArrayFromJSON<::uint64_t>("[0,1,2,0,1,2]"));

        INFO(result << "\n!=\n" << expected);
        REQUIRE(result.equals_(expected));
    }

    SECTION("Concatenate two DataFrames with duplicate keys")
    {
        auto df2 =
                pd::DataFrame(pd::NULL_INDEX, pair{ "a"s, vector{ 7L, 8L, 9L } }, pair{ "b"s, vector{ 10L, 11L, 12L } });

        auto concatenator = pd::Concatenator({ df1, df2 });

        auto result = concatenator(AxisType::Index);
        auto expected = pd::DataFrame(
                TableWithType<int64_t>{ { "a", { 1, 2, 3, 7, 8, 9 } }, { "b", { 4, 5, 6, 10, 11, 12 } } },
                arrow::ArrayFromJSON<::uint64_t>("[0,1,2,0,1,2]"));
        REQUIRE(result.equals_(expected));
    }

    SECTION("Concatenate two DataFrames with different types of duplicate keys")
    {
        auto df2 = pd::DataFrame(
                pd::NULL_INDEX,
                pair{ "a"s, vector<double>{ 7.0, 8.0, 9.0 } },
                pair{ "b"s, vector{ 10L, 11L, 12L } });

        auto concatenator = pd::Concatenator({ df1, df2 });

        auto result = concatenator(AxisType::Index);
        auto expected = pd::DataFrame(
                arrow::ArrayFromJSON<::uint64_t>("[0,1,2,0,1,2]"),
                pair{ "a"s, vector<double>{ 1, 2, 3, 7.0, 8.0, 9.0 } },
                pair{ "b"s, vector<::int64_t>{ 4, 5, 6, 10, 11, 12 } });
        REQUIRE(result.equals_(expected));
    }

    SECTION("Concatenate two DataFrames with duplicate keys and ignore_index = true")
    {
        auto df2 =
                pd::DataFrame(pd::NULL_INDEX, pair{ "a"s, vector{ 7L, 8L, 9L } }, pair{ "b"s, vector{ 10L, 11L, 12L } });

        auto concatenator = pd::Concatenator({ df1, df2 }, JoinType::Outer, true);

        auto result = concatenator(AxisType::Index);
        auto expected =
                pd::DataFrame(TableWithType<int64_t>{ { "a", { 1, 2, 3, 7, 8, 9 } }, { "b", { 4, 5, 6, 10, 11, 12 } } });
        REQUIRE(result.equals_(expected));
    }

    SECTION("Concatenate two DataFrames with duplicate keys and sort = true")
    {
        auto df2 =
                pd::DataFrame(pd::NULL_INDEX, pair{ "a"s, vector{ 7L, 8L, 9L } }, pair{ "b"s, vector{ 10L, 11L, 12L } });

        auto concatenator = pd::Concatenator({ df1, df2 }, JoinType::Outer, false, true);

        auto result = concatenator(AxisType::Index);
        auto expected = pd::DataFrame(
                ArrayTable{ { "a", arrow::ArrayFromJSON<int64_t>("[1,2,3,7,8,9]") },
                            { "b", arrow::ArrayFromJSON<int64_t>("[4,5,6,10,11,12]") } },
                arrow::ArrayFromJSON<::uint64_t>("[0,1,2,0,1,2]"));

        INFO(result << "\n!=\n" << expected);
        REQUIRE(result.equals_(expected));
    }

    SECTION("Concatenate DataFrames with JoinType::Inner")
    {
        auto df2 =
                pd::DataFrame(pd::NULL_INDEX, pair{ "a"s, vector{ 7L, 8L, 9L } }, pair{ "b"s, vector{ 10L, 11L, 12L } });
        auto concatenator = pd::Concatenator({ df1, df2 }, JoinType::Inner);

        auto result = concatenator(AxisType::Index);
        auto expected = pd::DataFrame(
                ArrayTable{
                        { "a", arrow::ArrayFromJSON<int64_t>("[1,2,3,7,8,9]") },
                        { "b", arrow::ArrayFromJSON<int64_t>("[4,5,6,10,11,12]") },
                },
                arrow::ArrayFromJSON<::uint64_t>("[0,1,2,0,1,2]"));

        INFO(result << "\n!=\n" << expected);
        REQUIRE(result.equals_(expected));
    }

    SECTION("Concatenate DataFrames with different indexes and JoinType::Inner")
    {
        auto df2 =
                pd::DataFrame(pd::NULL_INDEX, pair{ "a"s, vector{ 7L, 8L, 9L } }, pair{ "c"s, vector{ 10L, 11L, 12L } });
        auto concatenator = pd::Concatenator({ df1, df2 }, JoinType::Inner);

        auto result = concatenator(AxisType::Index);
        auto expected = pd::DataFrame(
                ArrayTable{
                        { "a", arrow::ArrayFromJSON<int64_t>("[1,2,3,7,8,9]") },
                },
                arrow::ArrayFromJSON<::uint64_t>("[0,1,2,0,1,2]"));

        INFO(result << "\n!=\n" << expected);
        REQUIRE(result.equals_(expected));
    }

    SECTION("Concatenate two DataFrames with some duplicate keys and sort = true")
    {
        auto df2 = pd::DataFrame(
                pd::NULL_INDEX,
                pair{ "a"s, vector{ 7L, 8L, 9L } },
                pair{ "b"s, vector{ 10L, 11L, 12L } },
                pair{ "e"s, vector{ 13L, 14L, 15L } });
        auto concatenator = pd::Concatenator({ df1, df2 }, JoinType::Outer, false, true);

        auto result = concatenator(AxisType::Index);
        auto expected = pd::DataFrame(
                ArrayTable{ { "a", arrow::ArrayFromJSON<int64_t>("[1,2,3,7,8,9]") },
                            { "b", arrow::ArrayFromJSON<int64_t>("[4,5,6,10,11,12]") },
                            { "e", arrow::ArrayFromJSON<int64_t>("[null,null,null,13,14,15]") } },
                arrow::ArrayFromJSON<::uint64_t>("[0,1,2,0,1,2]"));

        INFO(result << "\n!=\n" << expected);
        REQUIRE(result.equals_(expected));
    }

    SECTION("Concatenate two DataFrames with some duplicate keys and sort = false")
    {
        auto df2 = pd::DataFrame(
                pd::NULL_INDEX,
                pair{ "a"s, vector{ 7L, 8L, 9L } },
                pair{ "b"s, vector{ 10L, 11L, 12L } },
                pair{ "e"s, vector{ 13L, 14L, 15L } });

        auto concatenator = pd::Concatenator({ df1, df2 }, JoinType::Outer, false, false);

        auto result = concatenator(AxisType::Index);
        auto expected = pd::DataFrame(
                ArrayTable{ { "a", arrow::ArrayFromJSON<int64_t>("[1,2,3,7,8,9]") },
                            { "b", arrow::ArrayFromJSON<int64_t>("[4,5,6,10,11,12]") },
                            { "e", arrow::ArrayFromJSON<int64_t>("[null,null,null,13,14,15]") } },
                arrow::ArrayFromJSON<::uint64_t>("[0,1,2,0,1,2]"));

        INFO(result << "\n!=\n" << expected);
        REQUIRE(result.equals_(expected));
    }
}

// TEST_CASE("concatenating DataFrame with Series", "[concat]")
//{
//
//    pd::DataFrame df1{ pd::ArrayTable{
//                           { "s1", arrow::ArrayFromJSON<int>("[1,2,3]") },
//                           { "e", arrow::ArrayFromJSON<int>("[4,5,6]") } },
//                       arrow::ArrayFromJSON<int>("[2,1,4]") };
//
//    pd::Series s1{ arrow::ArrayFromJSON<int>("[1,2,3]"), arrow::ArrayFromJSON<int>("[0,1,2]"), "s1" };
//    pd::Series s2{ arrow::ArrayFromJSON<int>("[4,5,6]"), arrow::ArrayFromJSON<int>("[3,4,5]"), "s2" };
//    pd::Series s3{ arrow::ArrayFromJSON<int>("[7,8,9]"), arrow::ArrayFromJSON<int>("[6,7,8]") };
//
//    std::vector<NDFrameType> objs { s1, s2, df1, s3};
//    pd::DataFrame result = pd::concat(objs, AxisType::Columns);
//
//    pd::DataFrame expected_result{ pd::ArrayTable{
//        { "s1", arrow::ArrayFromJSON<int>("[1, 2, 3, null, null, null]") },
//        { "s2", arrow::ArrayFromJSON<int>("[null, null, null, 4, 5, 6]") },
//        { "s1", arrow::ArrayFromJSON<int>("[null, 2, 1, null, 3, null]") },
//        { "e", arrow::ArrayFromJSON<int>("[null, 5, 4, null, 6, null]") },
//        { "0", arrow::ArrayFromJSON<int>("[null, null, null, null, null, 7, 8, 9]") }
//    }};
//
//    INFO(result << "\n != \n" << expected_result);
//    REQUIRE(result.equals_(expected_result));
//
//}