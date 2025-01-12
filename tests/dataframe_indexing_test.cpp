//
// Created by adesola on 1/11/25.
//
#include "pandas_arrow.h"
#include "catch.hpp"


using namespace std::string_literals;


TEST_CASE("Test setIndex", "[DataFrame]")
{
    auto df = pd::DataFrame{ std::map<std::string, std::vector<int>>{ { "month", std::vector{ 1, 4, 7, 10 } },
                                                                      { "year", std::vector{ 2012, 2014, 2013, 2014 } },
                                                                      { "sale", std::vector{ 55, 40, 84, 31 } } } };

    auto month = df["month"];

    df = df.setIndex("month");
    REQUIRE(df.index().equals_(month));
}

TEST_CASE("Test DataFrame slice", "[DataFrame]")
{
    // Test slice with offset
    auto array =
            arrow::ArrayVector{ arrow::ArrayT<int>::Make({ 1, 2, 3, 4 }), arrow::ArrayT<int>::Make({ 5, 6, 7, 8 }) };

    auto table = arrow::RecordBatch::Make(
            arrow::schema({ arrow::field("a", arrow::int32()), arrow::field("b", arrow::int32()) }),
            4,
            array);

    pd::DataFrame df1(table);
    auto df2 = df1.slice(2);
    REQUIRE(df2.num_rows() == 2);
    REQUIRE(df2.num_columns() == 2);
    REQUIRE(df2.at(0, 0).as<int32_t>() == 3);
    REQUIRE(df2.at(0, 1).as<int32_t>() == 7);
    REQUIRE(df2.at(1, 0).as<int32_t>() == 4);
    REQUIRE(df2.at(1, 1).as<int32_t>() == 8);

    // Test slice with offset and columns
    auto df3 = df1.slice(1, { "a" });
    REQUIRE(df3.num_rows() == 3);
    REQUIRE(df3.num_columns() == 1);
    REQUIRE(df3.columnNames() == std::vector<std::string>{ "a" });
    REQUIRE(df3.at(0, 0).as<int32_t>() == 2);
    REQUIRE(df3.at(1, 0).as<int32_t>() == 3);
    REQUIRE(df3.at(2, 0).as<int32_t>() == 4);
}

TEST_CASE("Test Slice", "[DataFrame]")
{
    auto df = pd::DataFrame{ std::map<std::string, std::vector<int>>{ { "month", std::vector{ 1, 4, 7, 10 } },
                                                                      { "year", std::vector{ 2012, 2014, 2013, 2014 } },
                                                                      { "sale", std::vector{ 55, 40, 84, 31 } } } };

    auto s1 = df[pd::Slice{ 1, 3 }];
    auto s2 = df[pd::Slice{ -1, 4 }];
    auto s3 = df.slice(pd::Slice{ 2 }, { "month" });

    REQUIRE(s1["month"].equals(std::vector{ 4, 7 }));
    REQUIRE(s1["year"].equals(std::vector{ 2014, 2013 }));
    REQUIRE(s1["sale"].equals(std::vector{ 40, 84 }));

    REQUIRE(s2["month"].equals(std::vector{ 10 }));
    REQUIRE(s2["year"].equals(std::vector{ 2014 }));
    REQUIRE(s2["sale"].equals(std::vector{ 31 }));
}

TEST_CASE("Test DateTimeSlice", "[DataFrame]")
{
    auto df = pd::DataFrame{ std::map<std::string, std::vector<int>>{ { "month", std::vector{ 1, 4, 7, 10 } },
                                                                      { "year", std::vector{ 2012, 2014, 2013, 2014 } },
                                                                      { "sale", std::vector{ 55, 40, 84, 31 } } },
                             pd::date_range(date(2022, 10, 1), 4) };

    auto s1 = df[pd::Slice{ 1, 3 }];
    auto s2 = df[pd::Slice{ -1, 4 }];

    REQUIRE(s1["month"].equals(std::vector{ 4, 7 }));
    REQUIRE(s1["year"].equals(std::vector{ 2014, 2013 }));
    REQUIRE(s1["sale"].equals(std::vector{ 40, 84 }));

    REQUIRE(s2["month"].equals(std::vector{ 10 }));
    REQUIRE(s2["year"].equals(std::vector{ 2014 }));
    REQUIRE(s2["sale"].equals(std::vector{ 31 }));

    auto s3 = df[pd::DateSlice{ date(2022, 10, 2), date(2022, 10, 4) }];

    REQUIRE(s3["month"].equals(std::vector{ 4, 7 }));
    REQUIRE(s3["year"].equals(std::vector{ 2014, 2013 }));
    REQUIRE(s3["sale"].equals(std::vector{ 40, 84 }));
}

TEST_CASE("Test all math operators for DataFrame", "[math_operators]")
{
    auto index = pd::range(0UL, 3UL);

    pd::DataFrame df1(
            std::vector<std::vector<int>>{std::vector{1, 2, 3}, std::vector{4, 5, 6}, std::vector{7, 8, 9}},
            {"a", "b", "c"},
            arrow::ArrayT<std::string>::Make({"x"s, "y"s, "z"s}));


    pd::DataFrame df2(
            std::vector<std::vector<int>>{std::vector{2, 4, 6}, std::vector{8, 10, 12}, std::vector{14, 16, 18}},
            {"a", "b", "c"},
            arrow::ArrayT<std::string>::Make({"x"s, "y"s, "z"s}));

    // Test addition operator
    {
        auto df3 = df1 + df2;
        INFO(df3);
        for (int i = 0; i < 3; i++) {
            for (int j = 0; j < 3; j++) {
                REQUIRE(df3.at(i, j) == (df1.at(i, j) + df2.at(i, j)));
            }
        }
    }

    // Test subtraction operator
    {
        auto df3 = df1 - df2;
        INFO(df3);
        for (int i = 0; i < 3; i++) {
            for (int j = 0; j < 3; j++) {
                REQUIRE(df3.at(i, j) == (df1.at(i, j) - df2.at(i, j)));
            }
        }
    }

    // Test multiplication operator
    {
        auto df3 = df1 * df2;
        INFO(df3);
        for (int i = 0; i < 3; i++) {
            for (int j = 0; j < 3; j++) {
                REQUIRE(df3.at(i, j) == (df1.at(i, j) * df2.at(i, j)));
            }
        }
    }

    // Test division operator
    {
        auto df3 = df1 / df2;
        for (int i = 0; i < 3; i++) {
            for (int j = 0; j < 3; j++) {
                REQUIRE(df3.at(i, j) == (df1.at(i, j).as<int>() / df2.at(i, j).as<int>()));
            }
        }
    }
}

TEST_CASE("Test reindex vs reindex_async benchmark", "[.reindex]")
{
    // Create a test input Series
    auto rand = pd::random::RandomState(100);

    for (auto [length, message] : std::vector{ std::pair{ 1e4, "Small Data" }, { 5e6, "Big Data" } })
    {
        std::cout << message << "\n";
        DYNAMIC_SECTION(message)
        {
            auto inputIndex = pd::date_range(ptime(), length);
            pd::DataFrame inputDataFrame(
                    std::map<std::string, std::vector<int64_t>>{
                            { "col1", rand.randint(length, 0, 100) },
                            { "col2", rand.randint(length, 0, 100) },
                            { "col3", rand.randint(length, 0, 100) },
                            { "col4", rand.randint(length, 0, 100) },
                            { "col5", rand.randint(length, 0, 100) },
                            { "col6", rand.randint(length, 0, 100) },
                            { "col7", rand.randint(length, 0, 100) },
                            { "col8", rand.randint(length, 0, 100) },
                            { "col9", rand.randint(length, 0, 100) },
                            { "col10", rand.randint(length, 0, 100) },
                            { "col11", rand.randint(length, 0, 100) },
                            { "col12", rand.randint(length, 0, 100) },
                            { "col13", rand.randint(length, 0, 100) },
                    },
                    inputIndex);

            auto newIndex = pd::date_range(ptime() + minutes(1), int(length / 5));
            pd::DataFrame sortedOut{ nullptr, nullptr }, out{ nullptr, nullptr };

            auto start = std::chrono::high_resolution_clock::now();
            sortedOut = inputDataFrame.reindex(newIndex);
            auto end = std::chrono::high_resolution_clock::now();

            std::cout << "SINGLE THREAD: " << std::chrono::duration<double>(end - start).count() << " s.\n";

            start = std::chrono::high_resolution_clock::now();
            out = inputDataFrame.reindexAsync(newIndex);
            end = std::chrono::high_resolution_clock::now();

            std::cout << "MULTI THREAD: " << std::chrono::duration<double>(end - start).count() << " s.\n";
        }
    }
}

TEST_CASE("Test reindex function for DataFrame", "[reindex]")
{
    // Create a test input DataFrame
    auto inputIndex = arrow::ArrayT<::int64_t>::Make({ 1, 2, 3, 4, 5 });
    pd::DataFrame inputDataFrame(
            std::map<std::string, std::vector<int64_t>>{ { "col1", { 1, 2, 3, 4, 5 } }, { "col2", { 5, 4, 3, 2, 1 } } },
            inputIndex);

    // Create a new index array
    auto newIndex = arrow::ArrayT<::int64_t>::Make({ 1, 2, 4, 5, 6 });

    // Reindex the input DataFrame
    pd::DataFrame outputDataFrame = inputDataFrame.reindex(newIndex);

    // Check that the new index is correct
    INFO(outputDataFrame);
    INFO(newIndex->ToString());
    REQUIRE(outputDataFrame.indexArray()->Equals(newIndex));

    // Check that the values of the first column are correct
    auto expectedValues1 = arrow::ArrayT<::int64_t>::Make({ 1, 2, 4, 5, 0 }, { true, 1, 1, 1, 0 });
    REQUIRE(outputDataFrame["col1"].array()->Equals(expectedValues1));

    // Check that the values of the second column are correct
    auto expectedValues2 = arrow::ArrayT<::int64_t>::Make({ 5, 4, 2, 1, 0 }, { true, 1, 1, 1, 0 });
    REQUIRE(outputDataFrame["col2"].array()->Equals(expectedValues2));
}

TEST_CASE("Test reindex on datetime index", "[reindex]")
{
    // Create a test input Series
    pd::DataFrame df2(
            pd::date_range(date(2010, 1, 1), 6),
            std::pair{ "prices"s, std::vector<double>{ 100, 101, NAN, 100, 89, 88 } });

    pd::DataFrame expected{ pd::date_range(date(2009, 12, 29), 10),
                            std::pair{ "prices"s,
                                       std::vector<double>{ NAN, NAN, NAN, 100, 101, NAN, 100, 89, 88, NAN } } };

    auto output = df2.reindex(pd::date_range(date(2009, 12, 29), 10));

    INFO(output << " != " << expected);
    REQUIRE(output.equals_(expected));
}

TEST_CASE("Test drop_na function for DataFrame", "[drop_na]")
{
    using namespace std::string_literals;
    auto df = pd::DataFrame{
            arrow::ArrayT<::int64_t>::Make({ 1, 4, 5 }),
            std::pair{ "name"s, std::vector{ "Alfred"s, "Batman"s, "Catwoman"s } },
            std::pair{ "toy_size"s, std::vector{ std::numeric_limits<double>::quiet_NaN(), 100.2, std::numeric_limits<double>::quiet_NaN() } },
            std::pair{ "born"s, std::vector{ "1940-04-24"s, "1940-04-25"s, "1940-04-26"s } },
    };

    df["born"] = df["born"].to_datetime();

    df = df.drop_na();

    INFO(df);

    REQUIRE(df.shape() == pd::DataFrame::Shape{ 1, 3 });

    REQUIRE(df.at(0, 0).scalar->ToString() == "Batman");
    REQUIRE(df.at(0, 1).scalar->ToString() == "100.2");
    REQUIRE(df.at(0, 2).scalar->ToString() == "1940-04-25");
    REQUIRE(df.index()[0] == 4L);
}