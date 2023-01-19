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

using namespace std::string_literals;
TEST_CASE("Test describe with empty DataFrame", "[describe]") {
    pd::DataFrame df;
    auto desc = df.describe();
    REQUIRE(desc.shape() == pd::DataFrame::Shape{ 0, 0 });
    REQUIRE(desc.columns().empty());
    REQUIRE(desc.index().empty());
}

TEST_CASE("Test describe with all NA values", "[describe]")
{
    double NA = std::numeric_limits<double>::quiet_NaN();
    pd::DataFrame df(
        std::vector<std::vector<double>>{ std::vector{ NA, NA, NA },
                                          std::vector{ NA, NA, NA },
                                          std::vector{ NA, NA, NA } });
    df.setColumns({ "a", "b", "c" });
    df.setIndex(arrow::ArrayT<std::string>::Make({ "x"s, "y"s, "z"s }));

    auto desc = df.describe();
    REQUIRE(desc.shape() == pd::DataFrame::Shape{ 3, 6 });
    REQUIRE(
        desc.columns() ==
        std::vector<std::string>{ "count", "mean", "std", "min", "nunique", "max" });
    REQUIRE(desc.index().equals(std::vector<std::string>{ "a", "b", "c" }));

    for (int i = 0; i < 3; i++)
    {
        for (int j = 1; j < 3; j++)
        {
            REQUIRE(desc.at(i, j).as<double>() == 0);
        }

        for (int j : {0, 4})
        {
            REQUIRE(desc.at(i, j).as<::int64_t>() == 0);
        }

        for (int j : {3, 5})
        {
            REQUIRE_FALSE(desc.at(i, j).isValid());
        }
    }
}

TEST_CASE("Test describe with numeric columns", "[describe]")
{
    pd::DataFrame df(std::vector<std::vector<int>>{ std::vector{ 1, 2, 3 },
                                                    std::vector{ 4, 5, 6 },
                                                    std::vector{ 7, 8, 9 } });
    df.setColumns({ "a", "b", "c" });
    df.setIndex(arrow::ArrayT<std::string>::Make({ "x"s, "y"s, "z"s }));

    // Test default describe
    auto desc = df.describe(true, true);
    REQUIRE(desc.shape() == pd::DataFrame::Shape{ 3, 9 });
    REQUIRE(
        desc.columns() ==
        std::vector<std::string>{ "count",
                                  "mean",
                                  "std",
                                  "min",
                                  "nunique",
                                  "25%",
                                  "50%",
                                  "75%",
                                  "max" });
    REQUIRE(desc.index().equals(std::vector<std::string>{ "a", "b", "c" }));

    REQUIRE(desc.at("a", "count") == 3L);
    REQUIRE(desc.at("a", "mean") == 2.0);
    REQUIRE(desc.at("a", "std").as<double>() == 1.0);
    REQUIRE(desc.at("a", "min") == 1);
    REQUIRE(desc.at("a", "25%") == 1.5);
    REQUIRE(desc.at("a", "50%") == 2.0);
    REQUIRE(desc.at("a", "75%") == 2.5);
    REQUIRE(desc.at("a", "max") == 3);
    REQUIRE(desc.at("a", "nunique") == 3L);
}

TEST_CASE("Test describe with non-numeric columns", "[describe]")
{
    pd::DataFrame df(
        std::vector<std::vector<std::string>>{ std::vector<std::string>{ "a", "b", "c" },
                                               std::vector<std::string>{ "d", "e", "f" },
                                               std::vector<std::string>{ "g", "h", "i" } });
    df.setColumns({ "x", "y", "z" });
    df.setIndex(arrow::ArrayT<std::string>::Make({ "a"s, "b"s, "c"s }));

    REQUIRE_THROWS(df.describe());
    //    REQUIRE(desc.shape() == pd::DataFrame::Shape{ 3, 4 });
    //    REQUIRE(
    //        desc.columns() ==
    //        std::vector<std::string>{ "count", "nunique", "top", "freq" });
    //    REQUIRE(desc.index().equals(std::vector<std::string>{ "x", "y", "z" })); REQUIRE(desc.at("count", "x") == 3); REQUIRE(desc.at("nunique", "x") == 3); REQUIRE(desc.at("top", "x") == "a");
    //    REQUIRE(desc.at("freq", "x") == 1);
}

TEST_CASE("Test all math operators for DataFrame", "[math_operators]")
{
    pd::DataFrame df1(std::vector<std::vector<int>>{ std::vector{ 1, 2, 3 },
                                                     std::vector{ 4, 5, 6 },
                                                     std::vector{ 7, 8, 9 } });
    df1.setColumns({ "a", "b", "c" });
    df1.setIndex(arrow::ArrayT<std::string>::Make({ "x"s, "y"s, "z"s }));

    pd::DataFrame df2(std::vector<std::vector<int>>{ std::vector{ 2, 4, 6 },
                                                     std::vector{ 8, 10, 12 },
                                                     std::vector{ 14, 16, 18 } });
    df2.setColumns({ "a", "b", "c" });
    df2.setIndex(arrow::ArrayT<std::string>::Make({ "x"s, "y"s, "z"s }));

    // Test addition operator
    auto df3 = df1 + df2;
    for (int i = 0; i < 3; i++) {
        for (int j = 0; j < 3; j++) {
            REQUIRE(df3.at(i, j) == (df1.at(i, j).as<int>() + df2.at(i, j).as<int>()));
        }
    }

    // Test subtraction operator
    df3 = df1 - df2;
    for (int i = 0; i < 3; i++) {
        for (int j = 0; j < 3; j++) {
            REQUIRE(df3.at(i, j) == (df1.at(i, j).as<int>() - df2.at(i, j).as<int>()));
        }
    }

    // Test multiplication operator
    df3 = df1 * df2;
    for (int i = 0; i < 3; i++) {
        for (int j = 0; j < 3; j++) {
            REQUIRE(df3.at(i, j) == (df1.at(i, j).as<int>() * df2.at(i, j).as<int>()));
        }
    }

    // Test division operator
    df3 = df1 / df2;
    for (int i = 0; i < 3; i++) {
        for (int j = 0; j < 3; j++) {
            REQUIRE(df3.at(i, j) == (df1.at(i, j).as<int>() / df2.at(i, j).as<int>()));
        }
    }
}

TEST_CASE("Test argsort", "[argsort]")
{
    // Create a test DataFrame with some sample data
    pd::DataFrame df = std::vector<std::vector<int>>{ { 1, 2, 3 }, { 4, 5, 6 }, { 7, 8, 9 } };
    df.setColumns({ "a", "b", "c" });

    // Test argsort with ascending = true
    auto sorted = df.argsort({"a"}, true);
    REQUIRE(sorted.count() == 3);
    REQUIRE(sorted[0] == 0);
    REQUIRE(sorted[1] == 1);
    REQUIRE(sorted[2] == 2);

    // Test argsort with ascending = false
    sorted = df.argsort({"a"}, false);
    REQUIRE(sorted.count() == 3);
    REQUIRE(sorted[0] == 2);
    REQUIRE(sorted[1] == 1);
    REQUIRE(sorted[2] == 0);

    // Test argsort with multiple fields
    sorted = df.argsort({"a", "b"}, true);
    REQUIRE(sorted.count() == 3);
    REQUIRE(sorted[0] == 0);
    REQUIRE(sorted[1] == 1);
    REQUIRE(sorted[2] == 2);
}

TEST_CASE("Test sort_index with ascending=true and ignore_index=false", "[sort_index]")
{
    pd::DataFrame df(std::vector<std::vector<int>>{ { 1, 2, 3 },
                                                    { 4, 5, 6 },
                                                    { 7, 8, 9 } });
    df.setColumns({ "a", "b", "c" });
    df.setIndex(arrow::ArrayT<std::string>::Make({ "c"s, "b"s, "a"s }));

    auto sorted = df.sort_index(true, false);

    REQUIRE(sorted.index().equals(std::vector<std::string>{"a","b","c"}));

    REQUIRE(sorted.at(0,0) == df.at(2,0));
    REQUIRE(sorted.at(1,0) == df.at(1,0));
    REQUIRE(sorted.at(2,0) == df.at(0,0));
    REQUIRE(sorted.at(0,1) == df.at(2,1));
    REQUIRE(sorted.at(1,1) == df.at(1,1));
    REQUIRE(sorted.at(2,1) == df.at(0,1));
    REQUIRE(sorted.at(0,2) == df.at(2,2));
    REQUIRE(sorted.at(1,2) == df.at(1,2));
    REQUIRE(sorted.at(2,2) == df.at(0,2));
}

TEST_CASE("Test sort_index with ascending=false and ignore_index=true", "[sort_index]")
{
    pd::DataFrame df(std::vector<std::vector<int>>{ { 1, 2, 3 },
                                                    { 4, 5, 6 },
                                                    { 7, 8, 9 } });
    df.setColumns({ "a", "b", "c" });
    df.setIndex(arrow::ArrayT<std::string>::Make({ "x"s, "y"s, "z"s }));

    auto sorted = df.sort_index(false, true);
    REQUIRE(sorted.shape() == df.shape());
    REQUIRE(sorted.columns() == df.columns());
    REQUIRE(sorted.index().equals(std::vector<std::string>{ "z", "y", "x" }));
    REQUIRE(sorted.at(2, 0) == 1);
    REQUIRE(sorted.at(1, 1) == 5);
    REQUIRE(sorted.at(0, 2) == 9);
}

TEST_CASE("Test sort_values with by and ascending=false and ignore_index=true", "[sort_values]")
{
    pd::DataFrame df(std::vector<std::vector<int>>{ { 1, 2, 3 },
                                                    { 4, 5, 6 },
                                                    { 7, 8, 9 } });
    df.setColumns({ "a", "b", "c" });
    df.setIndex(arrow::ArrayT<std::string>::Make({ "x", "y", "z" }));

    auto sorted = df.sort_values({"a","b"},false, true);

    REQUIRE(sorted.shape() == pd::DataFrame::Shape{ 3, 3 });
    REQUIRE(sorted.columns() == std::vector<std::string>{"a","b","c"});

    //check rows are in descending order of column "a"
    REQUIRE(sorted.at(0, "a") == 3);
    REQUIRE(sorted.at(1, "a") == 2);
    REQUIRE(sorted.at(2, "a") == 1);

    //check rows are in descending order of column "b"
    REQUIRE(sorted.at(0, "b") == 6);
    REQUIRE(sorted.at(1, "b") == 5);
    REQUIRE(sorted.at(2, "b") == 4);

    //check index is ignored
    REQUIRE(sorted.index().equals(std::vector<::uint64_t>{ 0, 1, 2 }));
}

TEST_CASE("Test DataFrame coalesce", "[DataFrame]")
{
    pd::DataFrame df(std::vector<std::vector<int>>{ { 1, 2 },
                                                    { 3, 4 },
                                                    { 5, 6 } });
    df.setColumns({ "a", "b" });

    // Test coalesce() method
    pd::Series series = df.coalesce();
    REQUIRE(series.shape() == pd::Series::Shape{ 6 });
    REQUIRE(series.indexArray()->Equals(
        arrow::ArrayT<::uint64_t>::Make({ 0, 1, 2, 3, 4, 5 })));
    REQUIRE(series.at(0) == 1);
    REQUIRE(series.at(1) == 2);
    REQUIRE(series.at(2) == 3);
    REQUIRE(series.at(3) == 4);
    REQUIRE(series.at(4) == 5);
    REQUIRE(series.at(5) == 6);

    // Test coalesce(std::vector<std::string> const& columns) method
    pd::Series series2 = df.coalesce({"a"});
    REQUIRE(series2.shape() == pd::Series::Shape{ 2 });
    REQUIRE(series2.indexArray()->Equals(
        arrow::ArrayT<::uint64_t>::Make({ 1, 2 })));
    REQUIRE(series2.at(0) == 1);
    REQUIRE(series2.at(1) == 3);
    REQUIRE(series2.at(2) == 5);
}

TEST_CASE("makeGroups() works with a single column key", "[makeGroups]") {
    // Create a DataFrame with a single column key
    auto df = pd::DataFrame{
        pd::range(0, 10),
        std::pair{"id"s, std::vector{"allen"s, "victor"s, "hannah"s, "allen"s,
                                       "victor"s, "hannah"s, "allen"s,
                                       "victor"s, "hannah"s, "allen"s}},
        std::pair{"age"s,  std::vector{16, 10,  10, 20,
                                        30, 40, 15,
                                        25, 35, 45}}
    };
    pd::GroupBy groupby("id", df);
    // Verify that the groups are created correctly
    REQUIRE(groupby.groupSize() == 3);
    REQUIRE(groupby.group("allen").size() == 2);
    REQUIRE(groupby.group("victor").size() == 2);
    REQUIRE(groupby.group("hannah").size() == 2);

    for(int i = 0; i < 2; i++)
    {
        REQUIRE(groupby.group("allen").at(0)->length() == 4);
        REQUIRE(groupby.group("victor").at(i)->length()  == 3);
        REQUIRE(groupby.group("hannah").at(i)->length()  == 3);
    }

    REQUIRE(groupby.unique()->length() == 3);
    REQUIRE( pd::Scalar( groupby.unique()->GetScalar(0) ).as<std::string>() == "allen" );
    REQUIRE( pd::Scalar( groupby.unique()->GetScalar(1) ).as<std::string>() == "victor" );
    REQUIRE( pd::Scalar( groupby.unique()->GetScalar(2) ).as<std::string>() == "hannah" );

    auto allen_group = groupby.group("allen");

    REQUIRE( pd::Scalar( allen_group[0]->GetScalar(0) ).as<std::string>() == "allen");
    REQUIRE( pd::Scalar( allen_group[0]->GetScalar(1) ).as<std::string>() == "allen");
    REQUIRE( pd::Scalar( allen_group[0]->GetScalar(2) ).as<std::string>() == "allen");
    REQUIRE( pd::Scalar( allen_group[0]->GetScalar(3) ).as<std::string>() == "allen");
    REQUIRE( pd::Scalar( allen_group[1]->GetScalar(0) ).as<int>() == 16);
    REQUIRE( pd::Scalar( allen_group[1]->GetScalar(1) ).as<int>() == 20);
    REQUIRE( pd::Scalar( allen_group[1]->GetScalar(2) ).as<int>() == 15);
    REQUIRE( pd::Scalar( allen_group[1]->GetScalar(3) ).as<int>() == 45);

    auto hannah_group = groupby.group("hannah");
    REQUIRE( pd::Scalar( hannah_group[0]->GetScalar(0) ).as<std::string>() == "hannah");
    REQUIRE( pd::Scalar( hannah_group[0]->GetScalar(1) ).as<std::string>() == "hannah");
    REQUIRE( pd::Scalar( hannah_group[0]->GetScalar(2) ).as<std::string>() == "hannah");
    REQUIRE( pd::Scalar( hannah_group[1]->GetScalar(0) ).as<int>() == 10);
    REQUIRE( pd::Scalar( hannah_group[1]->GetScalar(1) ).as<int>() == 40);
    REQUIRE( pd::Scalar( hannah_group[1]->GetScalar(2) ).as<int>() == 35);

    auto victor_group = groupby.group("victor");
    REQUIRE( pd::Scalar( victor_group[0]->GetScalar(0) ).as<std::string>() == "victor");
    REQUIRE( pd::Scalar( victor_group[0]->GetScalar(1) ).as<std::string>() == "victor");
    REQUIRE( pd::Scalar( victor_group[0]->GetScalar(2) ).as<std::string>() == "victor");
    REQUIRE( pd::Scalar( victor_group[1]->GetScalar(0) ).as<int>() == 10);
    REQUIRE( pd::Scalar( victor_group[1]->GetScalar(1) ).as<int>() == 30);
    REQUIRE( pd::Scalar( victor_group[1]->GetScalar(2) ).as<int>() == 25);

}

TEST_CASE("makeGroups() works on dataframe with more than two column keys", "[makeGroups]")
{
    // Create a DataFrame with multiple column keys
    auto df = pd::DataFrame{
        pd::range(0, 10),
        std::pair{ "id"s,
                   std::vector{ "allen"s,
                                "victor"s,
                                "hannah"s,
                                "allen"s,
                                "victor"s,
                                "hannah"s,
                                "allen"s,
                                "victor"s,
                                "hannah"s,
                                "allen"s } },
        std::pair{ "gender"s,
                   std::vector{ "male"s,
                                "female"s,
                                "male"s,
                                "male"s,
                                "female"s,
                                "male"s,
                                "male"s,
                                "female"s,
                                "male"s,
                                "male"s } },
        std::pair{ "age"s,
                   std::vector{ 16, 10, 10, 20, 30, 40, 15, 25, 35, 45 } }
    };
    pd::GroupBy groupby("gender", df);

    SECTION("groupSize() is valid")
    {
        REQUIRE(groupby.groupSize() == 2);
        REQUIRE(groupby.group("male").size() == 3);
        REQUIRE(groupby.group("female").size() == 3);

        for (int i = 0; i < 3; i++)
        {
            REQUIRE(groupby.group("male").at(i)->length() == 7);
            REQUIRE(groupby.group("female").at(i)->length() == 3);
        }

        REQUIRE(groupby.unique()->length() == 2);
    }

    SECTION("grouper selected correct unique_keys")
    {
        REQUIRE(
            pd::Scalar(groupby.unique()->GetScalar(0)).as<std::string>() ==
            "male");
        REQUIRE(
            pd::Scalar(groupby.unique()->GetScalar(1)).as<std::string>() ==
            "female");
    }


    SECTION("grouper created groups correctly")
    {
        auto male_group = groupby.group("male");

        REQUIRE(
            pd::Scalar(male_group[0]->GetScalar(0)).as<std::string>() ==
            "allen");
        REQUIRE(
            pd::Scalar(male_group[0]->GetScalar(1)).as<std::string>() ==
            "hannah");
        REQUIRE(
            pd::Scalar(male_group[0]->GetScalar(2)).as<std::string>() ==
            "allen");
        REQUIRE(
            pd::Scalar(male_group[0]->GetScalar(3)).as<std::string>() ==
            "hannah");
        REQUIRE(
            pd::Scalar(male_group[0]->GetScalar(4)).as<std::string>() ==
            "allen");
        REQUIRE(
            pd::Scalar(male_group[0]->GetScalar(5)).as<std::string>() ==
            "hannah");
        REQUIRE(
            pd::Scalar(male_group[0]->GetScalar(6)).as<std::string>() ==
            "allen");

        for (int i = 0; i < 7; i++)
        {
            REQUIRE(
                pd::Scalar(male_group[1]->GetScalar(i)).as<std::string>() ==
                "male");
        }

        REQUIRE(pd::Scalar(male_group[2]->GetScalar(0)).as<int>() == 16);
        REQUIRE(pd::Scalar(male_group[2]->GetScalar(1)).as<int>() == 10);
        REQUIRE(pd::Scalar(male_group[2]->GetScalar(2)).as<int>() == 20);
        REQUIRE(pd::Scalar(male_group[2]->GetScalar(3)).as<int>() == 40);
        REQUIRE(pd::Scalar(male_group[2]->GetScalar(4)).as<int>() == 15);
        REQUIRE(pd::Scalar(male_group[2]->GetScalar(5)).as<int>() == 35);
        REQUIRE(pd::Scalar(male_group[2]->GetScalar(6)).as<int>() == 45);

        auto female_group = groupby.group("female");
        REQUIRE(
            pd::Scalar(female_group[0]->GetScalar(0)).as<std::string>() ==
            "victor");
        REQUIRE(
            pd::Scalar(female_group[0]->GetScalar(1)).as<std::string>() ==
            "victor");
        REQUIRE(
            pd::Scalar(female_group[0]->GetScalar(2)).as<std::string>() ==
            "victor");

        for (int i = 0; i < 3; i++)
        {
            REQUIRE(
                pd::Scalar(female_group[1]->GetScalar(i)).as<std::string>() ==
                "female");
        }

        REQUIRE(pd::Scalar(female_group[2]->GetScalar(0)).as<int>() == 10);
        REQUIRE(pd::Scalar(female_group[2]->GetScalar(1)).as<int>() == 30);
        REQUIRE(pd::Scalar(female_group[2]->GetScalar(2)).as<int>() == 25);
    }

    SECTION("grouper computes mean of each groups and merge")
    {
        auto age_mean = pd::ValidateAndReturn(groupby.mean("age"));
        REQUIRE(age_mean[0].as<double>() == Catch::Approx(25.857).epsilon(1e-3));
        REQUIRE(age_mean[1].as<double>() == Catch::Approx(21.667).epsilon(1e-3));
    }

    SECTION("grouper computes sum of each groups and merge")
    {
        auto age_sum = pd::ValidateAndReturn(groupby.sum("age"));
        REQUIRE(age_sum[0].as<int>() == 181);
        REQUIRE(age_sum[1].as<int>() == 65);
    }

    SECTION("grouper computes count of each groups and merge")
    {
        auto age_count = pd::ValidateAndReturn(groupby.count("age"));
        REQUIRE(age_count[0].as<::int64_t>() == 7);
        REQUIRE(age_count[1].as<::int64_t>() == 3);
    }

//    SECTION("grouper apply count on each groups and merge")
//    {
//        auto count = [](pd::DataFrame const& df)
//        {
//            std::cout << df << "\n";
//            return arrow::MakeScalar(df.num_rows());
//        };
//        auto age_count = pd::ValidateAndReturn(groupby.apply(count));
//        REQUIRE(age_count.at(0, 0) == 7L);
//        REQUIRE(age_count.at(1, 0) == 3L);
//    }
}