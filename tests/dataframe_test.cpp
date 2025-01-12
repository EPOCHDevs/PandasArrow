#include "pandas_arrow.h"
#include "catch.hpp"


TEST_CASE("Test DataFrame Initialization", "[DataFrame]")
{
    // Test initialization with arrow::RecordBatch
    auto array =
        arrow::ArrayVector{ arrow::ArrayT<int>::Make({ 1, 2, 3, 4 }), arrow::ArrayT<int>::Make({ 5, 6, 7, 8 }) };

    auto table = arrow::RecordBatch::Make(
        arrow::schema({ arrow::field("a", arrow::int32()), arrow::field("b", arrow::int32()) }),
        4,
        array);

    pd::DataFrame df1(table);
    REQUIRE(df1.num_rows() == 4);
    REQUIRE(df1.num_columns() == 2);
    REQUIRE(df1.columnNames() == std::vector<std::string>{ "a", "b" });

    // Test initialization with arrow::Schema and vector of arrow::ArrayData
    auto schema = arrow::schema({ arrow::field("c", arrow::int32()), arrow::field("d", arrow::int32()) });

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
    auto index = arrow::ArrayT<int>::Make({ 1, 2, 3, 4 });
    pd::DataFrame df3(
        index,
        std::pair("a"s, std::vector<int32_t>{ 5, 6, 7, 8 }),
        std::pair("b"s, std::vector<int32_t>{ 9, 10, 11, 12 }));

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

TEST_CASE("Test DataFrame operator[]", "[DataFrame]")
{
    // Create a DataFrame with columns "a" and "b"
    auto df = pd::DataFrame(std::map<std::string, std::vector<int32_t>>{ { "a", { 1, 2, 3 } }, { "b", { 4, 5, 6 } } });

    // Test operator[] with single column
    REQUIRE(df["a"].values<int32_t>() == std::vector<int32_t>{ 1, 2, 3 });

    // Test operator[] with multiple columns
    auto df2 = df[std::vector<std::string>{ "a", "b" }];
    REQUIRE(df2.shape() == std::array<int64_t, 2>{ 3, 2 });
    REQUIRE(df2.columnNames() == std::vector<std::string>{ "a", "b" });

    // Test operator[] with single row
    auto row = df[1];
    REQUIRE(row.num_columns() == 2);
    REQUIRE(row.at(0, "a").as<int32_t>() == 2);
    REQUIRE(row.at(0, "b").as<int32_t>() == 5);

    // Test at method
    REQUIRE(df.at(1, 0).as<int32_t>() == 2);
    REQUIRE(df.at(1, 1).as<int32_t>() == 5);
}

TEST_CASE("Test initialization with vector of vector", "[DataFrame]")
{
    std::vector<std::vector<int32_t>> table = { { 5, 6, 7, 8 }, { 9, 10, 11, 12 } };
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

TEST_CASE("Test initialization with unordered_map of column name and data", "[DataFrame]")
{
    std::map<std::string, std::vector<int32_t>> table2{ { "a", { 1, 2, 3, 4 } }, { "b", { 5, 6, 7, 8 } } };
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

TEST_CASE("Test initialization with std::map", "[DataFrame]")
{
    std::map<std::string, std::vector<int32_t>> map{ { "c", { 13, 14, 15, 16 } }, { "d", { 17, 18, 19, 20 } } };
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
    auto struct_array =
        arrow::StructArray::Make(
            arrow::ArrayVector{ arrow::ArrayT<int>::Make(map["c"]), arrow::ArrayT<int>::Make(map["d"]) },
            std::vector<std::string>{ "c", "d" })
            .MoveValueUnsafe();
    pd::DataFrame df7(struct_array, std::vector<std::string>{ "c", "d" });
    REQUIRE(df7.num_rows() == 4);
    REQUIRE(df7.num_columns() == 2);
    REQUIRE(df7.columnNames() == std::vector<std::string>{ "c", "d" });
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
    auto df = pd::DataFrame(std::map<std::string, std::vector<int32_t>>{ { "a", { 1, 2, 3 } }, { "b", { 4, 5, 6 } } });

    // Test rename
    df.rename(std::unordered_map<std::string, std::string>{ { "a", "c" }, { "b", "d" } });
    REQUIRE(df.columnNames() == std::vector<std::string>{ "c", "d" });

    // Test contains
    REQUIRE(df.contains("c") == true);
    REQUIRE(df.contains("d") == true);
    REQUIRE(df.contains("e") == false);

    // Test add_prefix
    df.add_prefix("prefix_");
    REQUIRE(df.columnNames() == std::vector<std::string>{ "prefix_c", "prefix_d" });

    // Test add_suffix
    df.add_suffix("_suffix");
    REQUIRE(df.columnNames() == std::vector<std::string>{ "prefix_c_suffix", "prefix_d_suffix" });

    // Test makeDefaultColumnNames
    auto default_column_names = pd::DataFrame::makeDefaultColumnNames(5);
    REQUIRE(default_column_names == std::vector<std::string>{ "0", "1", "2", "3", "4" });
}

TEST_CASE("Test head and tail", "[DataFrame]")
{
    auto df = pd::DataFrame{ std::map<std::string, std::vector<int>>{ { "month", std::vector{ 1, 4, 7, 10 } },
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

using namespace std::string_literals;
TEST_CASE("Test describe with empty DataFrame", "[describe]")
{
    pd::DataFrame df;
    auto desc = df.describe();
    REQUIRE(desc.shape() == pd::DataFrame::Shape{ 0, 0 });
    REQUIRE(desc.columns().empty());
    REQUIRE(desc.index().empty());
}

TEST_CASE("Test describe with all NA values", "[describe]")
{
    double NA = std::numeric_limits<double>::quiet_NaN();
    pd::DataFrame df(std::vector<std::vector<double>>{std::vector{NA, NA, NA},
                                                      std::vector{NA, NA, NA},
                                                      std::vector{NA, NA, NA}},
                     {"a", "b", "c"},
                     arrow::ArrayT<std::string>::Make({"x"s, "y"s, "z"s}));
    auto desc = df.describe();
    REQUIRE(desc.shape() == pd::DataFrame::Shape{3, 6});
    REQUIRE(desc.columns() == std::vector<std::string>{"count", "mean", "std", "min", "nunique", "max"});
    REQUIRE(desc.index().equals(std::vector<std::string>{"a", "b", "c"}));

    for (int i = 0; i < 3; i++) {
        INFO(desc << "\n" << "i = " << i);

        REQUIRE(desc.at(i, 1).isValid());
        REQUIRE(desc.at(i, 2).isValid());

        for (int j: {0, 4}) {
            REQUIRE(desc.at(i, j).as<::int64_t>() == 0);
        }

        for (int j: {3, 5}) {
            REQUIRE_FALSE(desc.at(i, j).isValid());
        }
    }
}

TEST_CASE("Test describe with numeric columns", "[describe]")
{
    pd::DataFrame df(
        std::vector<std::vector<int>>{ std::vector{ 1, 2, 3 }, std::vector{ 4, 5, 6 }, std::vector{ 7, 8, 9 } },
    { "a", "b", "c" });
    df.setIndex(arrow::ArrayT<std::string>::Make({ "x"s, "y"s, "z"s }));

    // Test default describe
    auto desc = df.describe(true, true);
    REQUIRE(desc.shape() == pd::DataFrame::Shape{ 3, 9 });
    REQUIRE(
        desc.columns() ==
        std::vector<std::string>{ "count", "mean", "std", "min", "nunique", "25%", "50%", "75%", "max" });
    REQUIRE(desc.index().equals(std::vector<std::string>{ "a", "b", "c" }));

    REQUIRE(desc.at("a", "count").as<int64_t>() == 3L);
    REQUIRE(desc.at("a", "mean").as<double>() == 2.0);
    REQUIRE(desc.at("a", "std").as<double>() == 1.0);
    REQUIRE(desc.at("a", "min").as<int>()  == 1);
    REQUIRE(desc.at("a", "25%").as<double>()  == 1.5);
    REQUIRE(desc.at("a", "50%").as<double>()  == 2.0);
    REQUIRE(desc.at("a", "75%").as<double>()  == 2.5);
    REQUIRE(desc.at("a", "max").as<int>()  == 3);
    REQUIRE(desc.at("a", "nunique").as<int64_t>()  == 3L);
}

TEST_CASE("Test describe with non-numeric columns", "[describe]")
{
    pd::DataFrame df(std::vector<std::vector<std::string>>{ std::vector<std::string>{ "a", "b", "c" },
                                                            std::vector<std::string>{ "d", "e", "f" },
                                                            std::vector<std::string>{ "g", "h", "i" } },
                     { "x", "y", "z" });
    df.setIndex(arrow::ArrayT<std::string>::Make({ "a"s, "b"s, "c"s }));

    REQUIRE_THROWS(df.describe());
    //    REQUIRE(desc.shape() == pd::DataFrame::Shape{ 3, 4 });
    //    REQUIRE(
    //        desc.columns() ==
    //        std::vector<std::string>{ "count", "nunique", "top", "freq" });
    //    REQUIRE(desc.index().equals(std::vector<std::string>{ "x", "y", "z" })); REQUIRE(desc.at("count", "x") == 3);
    //    REQUIRE(desc.at("nunique", "x") == 3); REQUIRE(desc.at("top", "x") == "a"); REQUIRE(desc.at("freq", "x") ==
    //    1);
}

TEST_CASE("Test argsort", "[argsort]")
{
    // Create a test DataFrame with some sample data
    pd::DataFrame df{ std::vector<std::vector<int>>{ { 1, 2, 3 }, { 4, 5, 6 }, { 7, 8, 9 } }, { "a", "b", "c" } };

    // Test argsort with ascending = true
    auto sorted = df.argsort({ "a" }, true);
    REQUIRE(sorted.count() == 3);
    REQUIRE(sorted[0] == 0ul);
    REQUIRE(sorted[1] == 1ul);
    REQUIRE(sorted[2] == 2ul);

    // Test argsort with ascending = false
    sorted = df.argsort({ "a" }, false);
    REQUIRE(sorted.count() == 3);
    REQUIRE(sorted[0] == 2ul);
    REQUIRE(sorted[1] == 1ul);
    REQUIRE(sorted[2] == 0ul);

    // Test argsort with multiple fields
    sorted = df.argsort({ "a", "b" }, true);
    REQUIRE(sorted.count() == 3);
    REQUIRE(sorted[0] == 0ul);
    REQUIRE(sorted[1] == 1ul);
    REQUIRE(sorted[2] == 2ul);
}

TEST_CASE("Test sort_index with ascending=true and ignore_index=false", "[sort_index]")
{
    pd::DataFrame df(
        std::vector<std::vector<int>>{ { 1, 2, 3 }, { 4, 5, 6 }, { 7, 8, 9 } },
        { "a", "b", "c" },
        arrow::ArrayT<std::string>::Make({ "c"s, "b"s, "a"s }));

    auto sorted = df.sort_index(true, false);

    REQUIRE(sorted.index().equals(std::vector<std::string>{ "a", "b", "c" }));

    REQUIRE(sorted.at(0, 0) == df.at(2, 0));
    REQUIRE(sorted.at(1, 0) == df.at(1, 0));
    REQUIRE(sorted.at(2, 0) == df.at(0, 0));
    REQUIRE(sorted.at(0, 1) == df.at(2, 1));
    REQUIRE(sorted.at(1, 1) == df.at(1, 1));
    REQUIRE(sorted.at(2, 1) == df.at(0, 1));
    REQUIRE(sorted.at(0, 2) == df.at(2, 2));
    REQUIRE(sorted.at(1, 2) == df.at(1, 2));
    REQUIRE(sorted.at(2, 2) == df.at(0, 2));
}

TEST_CASE("Test sort_index with ascending=false and ignore_index=true", "[sort_index]")
{
    pd::DataFrame df(std::vector<std::vector<int>>{ { 1, 2, 3 }, { 4, 5, 6 }, { 7, 8, 9 } }, { "a", "b", "c" });
    df = df.setIndex(arrow::ArrayT<std::string>::Make({ "x"s, "y"s, "z"s }));

    INFO(df);

    auto sorted = df.sort_index(false, true);

    INFO(sorted);

    REQUIRE(sorted.shape() == df.shape());
    REQUIRE(sorted.columns() == df.columns());
    REQUIRE(sorted.index().equals(std::vector<::uint64_t>{ 0, 1, 2 }));
    REQUIRE(sorted.at(2, 0) == 1);
    REQUIRE(sorted.at(1, 1) == 5);
    REQUIRE(sorted.at(0, 2) == 9);
}

TEST_CASE("Test sort_index with ascending=false and ignore_index=false", "[sort_index]")
{
    pd::DataFrame df(std::vector<std::vector<int>>{ { 1, 2, 3 }, { 4, 5, 6 }, { 7, 8, 9 } }, { "a", "b", "c" });
    df = df.setIndex(arrow::ArrayT<std::string>::Make({ "x"s, "y"s, "z"s }));

    INFO(df);

    auto sorted = df.sort_index(false, false);

    INFO(sorted);

    REQUIRE(sorted.shape() == df.shape());
    REQUIRE(sorted.columns() == df.columns());
    REQUIRE(sorted.index().equals(std::vector<std::string>{ "z", "y", "x" }));
    REQUIRE(sorted.at(2, 0) == 1);
    REQUIRE(sorted.at(1, 1) == 5);
    REQUIRE(sorted.at(0, 2) == 9);
}

TEST_CASE("Test sort_values with by and ascending=false and ignore_index=true", "[sort_values]")
{
    pd::DataFrame df(std::vector<std::vector<int>>{ { 1, 2, 3 }, { 4, 5, 6 }, { 7, 8, 9 } },
                     { "a", "b", "c" });
    df = df.setIndex(arrow::ArrayT<std::string>::Make({ "x", "y", "z" }));

    auto sorted = df.sort_values({ "a", "b" }, false, true);

    REQUIRE(sorted.shape() == pd::DataFrame::Shape{ 3, 3 });
    REQUIRE(sorted.columns() == std::vector<std::string>{ "a", "b", "c" });

    // check rows are in descending order of column "a"
    REQUIRE(sorted.at(0, "a") == 3);
    REQUIRE(sorted.at(1, "a") == 2);
    REQUIRE(sorted.at(2, "a") == 1);

    // check rows are in descending order of column "b"
    REQUIRE(sorted.at(0, "b") == 6);
    REQUIRE(sorted.at(1, "b") == 5);
    REQUIRE(sorted.at(2, "b") == 4);

    // check index is ignored
    REQUIRE(sorted.index().equals(std::vector<::uint64_t>{ 0, 1, 2 }));
}

//  Each row of the output will be the corresponding value of the
//  first input which is non-null for that row, otherwise null.
TEST_CASE("Test DataFrame coalesce", "[DataFrame]")
{
    pd::DataFrame df(std::vector<std::vector<int>>{ { 1, 2 }, { 3, 4 }, { 5, 6 } }, { "a", "b", "c" });

    // Test coalesce() method
    pd::Series series = df.coalesce();
    INFO(series);
    REQUIRE(series.shape() == pd::Series::Shape{ 2 });
    REQUIRE(series.at(0) == 1);
    REQUIRE(series.at(1) == 2);

    // Test coalesce(std::vector<std::string> const& columns) method
    pd::Series series2 = df.coalesce({ "b", "c" });
    REQUIRE(series2.shape() == pd::Series::Shape{ 2 });
    REQUIRE(series2.at(0) == 3);
    REQUIRE(series2.at(1) == 4);
}

TEST_CASE("Test Transpose function", "[transpose]")
{
    using namespace pd;
    using std::pair;

    SECTION("Square DataFrame with homogeneous dtype", "[transpose]")
    {
        DataFrame df1(std::map<std::string, std::vector<int>>{ { "col1", { 1, 2 } }, { "col2", { 3, 4 } } });

        REQUIRE(df1.transpose().equals_(
            DataFrame{ std::map<std::string, std::vector<int>>{ { "0", { 1, 3 } }, { "1", { 2, 4 } } },
                       arrow::ArrayT<std::string>::Make({ "col1", "col2" }) }));
    }

    SECTION("Non-square DataFrame with mixed dtypes", "[transpose]")
    {
        DataFrame df1(
            pd::ArrayPtr{ nullptr },
            pair{ "name"s, std::vector{ "Alice"s, "Bob"s } },
            pair{ "score"s, std::vector{ 9.5, 8.0 } },
            pair{ "employed"s, std::vector{ false, true } },
            pair{ "kids"s, std::vector{ 0L, 0L } });

        REQUIRE(df1.transpose().equals_(
            DataFrame{ std::map<std::string, std::vector<std::string>>{ { "0", { "Alice", "9.5", "false", "0" } },
                                                                        { "1", { "Bob", "8", "true", "0" } } },
                       arrow::ArrayT<std::string>::Make({ "name", "score", "employed", "kids" }) }));
    }
}