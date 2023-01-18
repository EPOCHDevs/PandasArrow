#include "../pandas_arrow.h"
#include "catch.hpp"


TEST_CASE("Test DataFrame Initialization", "[DataFrame]")
{
    // Test initialization with arrow::RecordBatch
    auto array =  arrow::ArrayVector {
        arrow::ArrayT<int>::Make({ 1, 2, 3, 4 }),
            arrow::ArrayT<int>::Make({ 5, 6, 7, 8 }) };

    auto table = arrow::RecordBatch::Make(
        arrow::schema({ arrow::field("a", arrow::int32()),
                        arrow::field("b", arrow::int32()) }),
        4, array);

    pd::DataFrame df1(table);
    REQUIRE(df1.num_rows() == 4);
    REQUIRE(df1.num_columns() == 2);
    REQUIRE(df1.columnNames() == std::vector<std::string>{ "a", "b" });

    // Test initialization with arrow::Schema and vector of arrow::ArrayData
    auto schema = arrow::schema({ arrow::field("c", arrow::int32()),
                                  arrow::field("d", arrow::int32()) });

    auto array_data1 = array[0]->data();

    auto array_data2 = array[1]->data();

    pd::DataFrame df2(schema, 4, { array_data1, array_data2 });
    REQUIRE(df2.num_rows() == 4);
    REQUIRE(df2.num_columns() == 2);
    REQUIRE(df2.columnNames() == std::vector<std::string>{ "c", "d" });
    REQUIRE(df2.at(0, 0).as<int32_t>() == 1);
    REQUIRE(df2.at(0, 1).as<int32_t>() == 5);
    REQUIRE(df2.at(1, 0).as<int32_t>() == 2);
    REQUIRE(df2.at(1, 1).as<int32_t>() == 6);
    REQUIRE(df2.at(2, 0).as<int32_t>() == 3);
    REQUIRE(df2.at(2, 1).as<int32_t>() == 7);
}

TEST_CASE("Test DataFrame Initialization with std::array", "[DataFrame]")
{
    std::array<std::string, 2> columns = { "a", "b" };
    std::vector<int32_t> a = { 1, 2, 3, 4 };
    std::vector<int32_t> b = { 5, 6, 7, 8 };
    auto index = arrow::ArrayT<int>::Make({ 1, 2, 3, 4 });

    pd::DataFrame df1(columns, index, std::move(a), std::move(b));
    REQUIRE(df1.num_rows() == 4);
    REQUIRE(df1.num_columns() == 2);
    REQUIRE(df1.columnNames() == std::vector<std::string>{ "a", "b" });
    REQUIRE(df1.at(0, 0).as<int32_t>() == 1);
    REQUIRE(df1.at(0, 1).as<int32_t>() == 5);
    REQUIRE(df1.at(1, 0).as<int32_t>() == 2);
    REQUIRE(df1.at(1, 1).as<int32_t>() == 6);
    REQUIRE(df1.at(2, 0).as<int32_t>() == 3);
    REQUIRE(df1.at(2, 1).as<int32_t>() == 7);
}

TEST_CASE("Test DataFrame Initialization with column name and data pairs", "[DataFrame]")
{
    using namespace std::string_literals;
    // Test initialization with column name and data pairs
    auto index = arrow::ArrayT<int>::Make({1, 2, 3, 4});
    pd::DataFrame df3(index,
                      std::pair("a"s, std::vector<int32_t>{5, 6, 7, 8}),
                      std::pair("b"s, std::vector<int32_t>{9, 10, 11, 12}));

    REQUIRE(df3.num_rows() == 4);
    REQUIRE(df3.num_columns() == 2);
    REQUIRE(df3.columnNames() == std::vector<std::string>{ "a", "b" });
    REQUIRE(df3.at(0, 0).as<int32_t>() == 5);
    REQUIRE(df3.at(0, 1).as<int32_t>() == 9);
    REQUIRE(df3.at(1, 0).as<int32_t>() == 6);
    REQUIRE(df3.at(1, 1).as<int32_t>() == 10);
    REQUIRE(df3.at(2, 0).as<int32_t>() == 7);
    REQUIRE(df3.at(2, 1).as<int32_t>() == 11);
}

TEST_CASE("Test DataFrame operator[]", "[DataFrame]") {
    // Create a DataFrame with columns "a" and "b"
    auto df = pd::DataFrame(
        std::map<std::string, std::vector<int32_t>>{{"a", {1, 2, 3}}, {"b", {4, 5, 6}}});

    // Test operator[] with single column
    REQUIRE(df["a"].values<int32_t>() == std::vector<int32_t>{1, 2, 3});

    // Test operator[] with multiple columns
    auto df2 = df[std::vector<std::string>{"a", "b"}];
    REQUIRE(df2.shape() == std::array<int64_t, 2>{3, 2});
    REQUIRE(df2.columnNames() == std::vector<std::string>{"a", "b"});

    // Test operator[] with single row
    auto row = df[1];
    REQUIRE(row.size() == 2);
    REQUIRE(row["a"].as<int32_t>() == 2);
    REQUIRE(row["b"].as<int32_t>() == 5);

    // Test at method
    REQUIRE(df.at(1, 0).as<int32_t>() == 2);
    REQUIRE(df.at(1, 1).as<int32_t>() == 5);
}

TEST_CASE("Test initialization with vector of vector", "[DataFrame]")
{
    std::vector<std::vector<int32_t>> table = { { 5, 6, 7, 8 },
                                                { 9, 10, 11, 12 } };
    std::vector<std::string> columns = { "a", "b" };
    auto index = arrow::ArrayT<int>::Make({ 1, 2, 3, 4 });
    pd::DataFrame df4(table, columns, index);
    REQUIRE(df4.num_rows() == 4);
    REQUIRE(df4.num_columns() == 2);
    REQUIRE(df4.columnNames() == columns);
    REQUIRE(df4.at(0, 0).as<int32_t>() == 5);
    REQUIRE(df4.at(0, 1).as<int32_t>() == 9);
    REQUIRE(df4.at(1, 0).as<int32_t>() == 6);
    REQUIRE(df4.at(1, 1).as<int32_t>() == 10);
    REQUIRE(df4.at(2, 0).as<int32_t>() == 7);
    REQUIRE(df4.at(2, 1).as<int32_t>() == 11);
    REQUIRE(df4.at(3, 0).as<int32_t>() == 8);
    REQUIRE(df4.at(3, 1).as<int32_t>() == 12);
}

TEST_CASE("Test initialization with unordered_map of column name and data",
          "[DataFrame]")
{
    std::map<std::string, std::vector<int32_t>> table2{
        {"a", {1, 2, 3, 4}},
        {"b", {5, 6, 7, 8}}
    };
    pd::DataFrame df5(table2);
    REQUIRE(df5.num_rows() == 4);
    REQUIRE(df5.num_columns() == 2);
    REQUIRE(df5.columnNames() == std::vector<std::string>{ "a", "b" });
    REQUIRE(df5.at(0, 0).as<int32_t>() == 1);
    REQUIRE(df5.at(0, 1).as<int32_t>() == 5);
    REQUIRE(df5.at(1, 0).as<int32_t>() == 2);
    REQUIRE(df5.at(1, 1).as<int32_t>() == 6);
    REQUIRE(df5.at(2, 0).as<int32_t>() == 3);
    REQUIRE(df5.at(2, 1).as<int32_t>() == 7);
    REQUIRE(df5.at(3, 0).as<int32_t>() == 4);
    REQUIRE(df5.at(3, 1).as<int32_t>() == 8);
}

TEST_CASE("Test initialization with std::map",
          "[DataFrame]")
{
    std::map<std::string, std::vector<int32_t>> map{
        { "c", { 13, 14, 15, 16 } },
        { "d", { 17, 18, 19, 20 } }
    };
    pd::DataFrame df6(map);
    REQUIRE(df6.num_rows() == 4);
    REQUIRE(df6.num_columns() == 2);
    REQUIRE(df6.columnNames() == std::vector<std::string>{ "c", "d" });
    REQUIRE(df6.at(0, 0).as<int32_t>() == 13);
    REQUIRE(df6.at(0, 1).as<int32_t>() == 17);
    REQUIRE(df6.at(1, 0).as<int32_t>() == 14);
    REQUIRE(df6.at(1, 1).as<int32_t>() == 18);
    REQUIRE(df6.at(2, 0).as<int32_t>() == 15);
    REQUIRE(df6.at(2, 1).as<int32_t>() == 19);
    REQUIRE(df6.at(3, 0).as<int32_t>() == 16);
    REQUIRE(df6.at(3, 1).as<int32_t>() == 20);

    // Test initialization with arrow::StructArray
    auto struct_array = arrow::StructArray::Make(
        arrow::ArrayVector{ arrow::ArrayT<int>::Make(map["c"]),
                            arrow::ArrayT<int>::Make(map["d"])},
        std::vector<std::string>{"c", "d"}).MoveValueUnsafe();
    pd::DataFrame df7(struct_array, std::vector<std::string>{ "c", "d" });
    REQUIRE(df7.num_rows() == 4);
    REQUIRE(df7.num_columns() == 2);
    REQUIRE(df7.columnNames() == std::vector<std::string>{  "c", "d" });
    REQUIRE(df7.at(0, 0).as<int32_t>() == 13);
    REQUIRE(df7.at(0, 1).as<int32_t>() == 17);
    REQUIRE(df7.at(1, 0).as<int32_t>() == 14);
    REQUIRE(df7.at(1, 1).as<int32_t>() == 18);
    REQUIRE(df7.at(2, 0).as<int32_t>() == 15);
    REQUIRE(df7.at(2, 1).as<int32_t>() == 19);
    REQUIRE(df7.at(3, 0).as<int32_t>() == 16);
    REQUIRE(df7.at(3, 1).as<int32_t>() == 20);
}

TEST_CASE("Test DataFrame Rename, Contains, Add Prefix and Suffix", "[DataFrame]")
{
    auto df = pd::DataFrame(
        std::map<std::string, std::vector<int32_t>>{ { "a", { 1, 2, 3 } },
                                                     { "b", { 4, 5, 6 } } });

    // Test rename
    df.rename(std::unordered_map<std::string, std::string>{ { "a", "c" },
                                                            { "b", "d" } });
    REQUIRE(df.columnNames() == std::vector<std::string>{ "c", "d" });

    // Test contains
    REQUIRE(df.contains("c") == true);
    REQUIRE(df.contains("d") == true);
    REQUIRE(df.contains("e") == false);

    // Test add_prefix
    df.add_prefix("prefix_");
    REQUIRE(
        df.columnNames() == std::vector<std::string>{ "prefix_c", "prefix_d" });

    // Test add_suffix
    df.add_suffix("_suffix");
    REQUIRE(
        df.columnNames() ==
        std::vector<std::string>{ "prefix_c_suffix", "prefix_d_suffix" });

    // Test makeDefaultColumnNames
    auto default_column_names = pd::DataFrame::makeDefaultColumnNames(5);
    REQUIRE(
        default_column_names ==
        std::vector<std::string>{ "0", "1", "2", "3", "4" });
}

TEST_CASE("Test DataFrame slice", "[DataFrame]")
{
    // Test slice with offset
    auto array = arrow::ArrayVector{ arrow::ArrayT<int>::Make({ 1, 2, 3, 4 }),
                                     arrow::ArrayT<int>::Make({ 5, 6, 7, 8 }) };

    auto table = arrow::RecordBatch::Make(
        arrow::schema({ arrow::field("a", arrow::int32()),
                        arrow::field("b", arrow::int32()) }),
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

TEST_CASE("Test setIndex", "[DataFrame]")
{
    auto df = pd::DataFrame{ std::map<std::string, std::vector<int>>{
        { "month", std::vector{ 1, 4, 7, 10 } },
        { "year", std::vector{ 2012, 2014, 2013, 2014 } },
        { "sale", std::vector{ 55, 40, 84, 31 } } } };

    auto month = df["month"];

    df = df.setIndex("month");
    REQUIRE(df.index().equals_(month));
}

TEST_CASE("Test head and tail", "[DataFrame]")
{
    auto df = pd::DataFrame{ std::map<std::string, std::vector<int>>{
        { "month", std::vector{ 1, 4, 7, 10 } },
        { "year", std::vector{ 2012, 2014, 2013, 2014 } },
        { "sale", std::vector{ 55, 40, 84, 31 } } } };

    auto h2 = df.head(2);
    auto t2 = df.tail(2);

    REQUIRE(h2["month"].equals(std::vector{ 1, 4 }));
    REQUIRE(h2["year"].equals(std::vector{ 2012, 2014 }));
    REQUIRE(h2["sale"].equals(std::vector{ 55, 40 }));

    REQUIRE(t2["month"].equals(std::vector{ 7, 10 }));
    REQUIRE(t2["year"].equals(std::vector{ 2013, 2014 }));
    REQUIRE(t2["sale"].equals(std::vector{ 84, 31 }));
}

TEST_CASE("Test Slice", "[DataFrame]")
{
    auto df = pd::DataFrame{ std::map<std::string, std::vector<int>>{
        { "month", std::vector{ 1, 4, 7, 10 } },
        { "year", std::vector{ 2012, 2014, 2013, 2014 } },
        { "sale", std::vector{ 55, 40, 84, 31 } } } };

    auto s1 = df.slice(pd::Slice{1, 3});
    auto s2 = df.slice(pd::Slice{-1, 4});
    auto s3 = df.slice(pd::Slice{2}, {"month"});

    REQUIRE(s1["month"].equals(std::vector{ 4, 7 }));
    REQUIRE(s1["year"].equals(std::vector{ 2014, 2013 }));
    REQUIRE(s1["sale"].equals(std::vector{ 40, 84 }));

    REQUIRE(s2["month"].equals(std::vector{ 10 }));
    REQUIRE(s2["year"].equals(std::vector{ 2014 }));
    REQUIRE(s2["sale"].equals(std::vector{ 31 }));


}

TEST_CASE("Test DateTimeSlice", "[DataFrame]")
{
    auto df =
        pd::DataFrame{ std::map<std::string, std::vector<int>>{
                           { "month", std::vector{ 1, 4, 7, 10 } },
                           { "year", std::vector{ 2012, 2014, 2013, 2014 } },
                           { "sale", std::vector{ 55, 40, 84, 31 } } },
                       pd::date_range(date(2022, 10, 1), 4) };

    auto s1 = df.slice(pd::Slice{ 1, 3 });
    auto s2 = df.slice(pd::Slice{ -1, 4 });

    REQUIRE(s1["month"].equals(std::vector{ 4, 7 }));
    REQUIRE(s1["year"].equals(std::vector{ 2014, 2013 }));
    REQUIRE(s1["sale"].equals(std::vector{ 40, 84 }));

    REQUIRE(s2["month"].equals(std::vector{ 10 }));
    REQUIRE(s2["year"].equals(std::vector{ 2014 }));
    REQUIRE(s2["sale"].equals(std::vector{ 31 }));

    auto s3 =
        df.slice(pd::DateTimeSlice{ date(2022, 10, 2), date(2022, 10, 4) });

    REQUIRE(s3["month"].equals(std::vector{ 4, 7 }));
    REQUIRE(s3["year"].equals(std::vector{ 2014, 2013 }));
    REQUIRE(s3["sale"].equals(std::vector{ 40, 84 }));
}