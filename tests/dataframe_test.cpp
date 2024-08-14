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

TEST_CASE("Test setIndex", "[DataFrame]")
{
    auto df = pd::DataFrame{ std::map<std::string, std::vector<int>>{ { "month", std::vector{ 1, 4, 7, 10 } },
                                                                      { "year", std::vector{ 2012, 2014, 2013, 2014 } },
                                                                      { "sale", std::vector{ 55, 40, 84, 31 } } } };

    auto month = df["month"];

    df = df.setIndex("month");
    REQUIRE(df.index().equals_(month));
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

TEST_CASE("Test Slice", "[DataFrame]")
{
    auto df = pd::DataFrame{ std::map<std::string, std::vector<int>>{ { "month", std::vector{ 1, 4, 7, 10 } },
                                                                      { "year", std::vector{ 2012, 2014, 2013, 2014 } },
                                                                      { "sale", std::vector{ 55, 40, 84, 31 } } } };

    auto s1 = df.slice(pd::Slice{ 1, 3 });
    auto s2 = df.slice(pd::Slice{ -1, 4 });
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

    auto s1 = df.slice(pd::Slice{ 1, 3 });
    auto s2 = df.slice(pd::Slice{ -1, 4 });

    REQUIRE(s1["month"].equals(std::vector{ 4, 7 }));
    REQUIRE(s1["year"].equals(std::vector{ 2014, 2013 }));
    REQUIRE(s1["sale"].equals(std::vector{ 40, 84 }));

    REQUIRE(s2["month"].equals(std::vector{ 10 }));
    REQUIRE(s2["year"].equals(std::vector{ 2014 }));
    REQUIRE(s2["sale"].equals(std::vector{ 31 }));

    auto s3 = df.slice(pd::DateTimeSlice{ date(2022, 10, 2), date(2022, 10, 4) });

    REQUIRE(s3["month"].equals(std::vector{ 4, 7 }));
    REQUIRE(s3["year"].equals(std::vector{ 2014, 2013 }));
    REQUIRE(s3["sale"].equals(std::vector{ 40, 84 }));
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
    pd::DataFrame df(std::vector<std::vector<double>>{ std::vector{ NA, NA, NA },
                                                       std::vector{ NA, NA, NA },
                                                       std::vector{ NA, NA, NA } });
    df.setColumns({ "a", "b", "c" });
    df.setIndex(arrow::ArrayT<std::string>::Make({ "x"s, "y"s, "z"s }));

    auto desc = df.describe();
    REQUIRE(desc.shape() == pd::DataFrame::Shape{ 3, 6 });
    REQUIRE(desc.columns() == std::vector<std::string>{ "count", "mean", "std", "min", "nunique", "max" });
    REQUIRE(desc.index().equals(std::vector<std::string>{ "a", "b", "c" }));

    for (int i = 0; i < 3; i++)
    {
        REQUIRE(std::isnan(desc.at(i, 1).as<double>()));
        REQUIRE(std::isnan(desc.at(i, 2).as<double>()));

        for (int j : { 0, 4 })
        {
            REQUIRE(desc.at(i, j).as<::int64_t>() == 0);
        }

        for (int j : { 3, 5 })
        {
            REQUIRE_FALSE(desc.at(i, j).isValid());
        }
    }
}

TEST_CASE("Test describe with numeric columns", "[describe]")
{
    pd::DataFrame df(
        std::vector<std::vector<int>>{ std::vector{ 1, 2, 3 }, std::vector{ 4, 5, 6 }, std::vector{ 7, 8, 9 } });
    df.setColumns({ "a", "b", "c" });
    df.setIndex(arrow::ArrayT<std::string>::Make({ "x"s, "y"s, "z"s }));

    // Test default describe
    auto desc = df.describe(true, true);
    REQUIRE(desc.shape() == pd::DataFrame::Shape{ 3, 9 });
    REQUIRE(
        desc.columns() ==
        std::vector<std::string>{ "count", "mean", "std", "min", "nunique", "25%", "50%", "75%", "max" });
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
    pd::DataFrame df(std::vector<std::vector<std::string>>{ std::vector<std::string>{ "a", "b", "c" },
                                                            std::vector<std::string>{ "d", "e", "f" },
                                                            std::vector<std::string>{ "g", "h", "i" } });
    df.setColumns({ "x", "y", "z" });
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

TEST_CASE("Test all math operators for DataFrame", "[math_operators]")
{
    pd::DataFrame df1(
        std::vector<std::vector<int>>{ std::vector{ 1, 2, 3 }, std::vector{ 4, 5, 6 }, std::vector{ 7, 8, 9 } });
    df1.setColumns({ "a", "b", "c" });
    df1.setIndex(arrow::ArrayT<std::string>::Make({ "x"s, "y"s, "z"s }));

    pd::DataFrame df2(
        std::vector<std::vector<int>>{ std::vector{ 2, 4, 6 }, std::vector{ 8, 10, 12 }, std::vector{ 14, 16, 18 } });
    df2.setColumns({ "a", "b", "c" });
    df2.setIndex(arrow::ArrayT<std::string>::Make({ "x"s, "y"s, "z"s }));

    // Test addition operator
    auto df3 = df1 + df2;
    for (int i = 0; i < 3; i++)
    {
        for (int j = 0; j < 3; j++)
        {
            REQUIRE(df3.at(i, j) == (df1.at(i, j).as<int>() + df2.at(i, j).as<int>()));
        }
    }

    // Test subtraction operator
    df3 = df1 - df2;
    for (int i = 0; i < 3; i++)
    {
        for (int j = 0; j < 3; j++)
        {
            REQUIRE(df3.at(i, j) == (df1.at(i, j).as<int>() - df2.at(i, j).as<int>()));
        }
    }

    // Test multiplication operator
    df3 = df1 * df2;
    for (int i = 0; i < 3; i++)
    {
        for (int j = 0; j < 3; j++)
        {
            REQUIRE(df3.at(i, j) == (df1.at(i, j).as<int>() * df2.at(i, j).as<int>()));
        }
    }

    // Test division operator
    df3 = df1 / df2;
    for (int i = 0; i < 3; i++)
    {
        for (int j = 0; j < 3; j++)
        {
            REQUIRE(df3.at(i, j) == (df1.at(i, j).as<int>() / df2.at(i, j).as<int>()));
        }
    }
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
    pd::DataFrame df(std::vector<std::vector<int>>{ { 1, 2, 3 }, { 4, 5, 6 }, { 7, 8, 9 } });
    df = df.setColumns({ "a", "b", "c" });
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
    pd::DataFrame df(std::vector<std::vector<int>>{ { 1, 2, 3 }, { 4, 5, 6 }, { 7, 8, 9 } });
    df = df.setColumns({ "a", "b", "c" });
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
    pd::DataFrame df(std::vector<std::vector<int>>{ { 1, 2, 3 }, { 4, 5, 6 }, { 7, 8, 9 } });
    df = df.setColumns({ "a", "b", "c" });
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

TEST_CASE("makeGroups() works with a single column key", "[makeGroups]")
{
    // Create a DataFrame with a single column key
    auto df = pd::DataFrame{ pd::range(0L, 10L),
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
                             std::pair{ "age"s, std::vector{ 16, 10, 10, 20, 30, 40, 15, 25, 35, 45 } } };
    pd::GroupBy groupby("id", df);
    // Verify that the groups are created correctly
    REQUIRE(groupby.groupSize() == 3);
    REQUIRE(groupby.group("allen").size() == 2);
    REQUIRE(groupby.group("victor").size() == 2);
    REQUIRE(groupby.group("hannah").size() == 2);

    for (int i = 0; i < 2; i++)
    {
        REQUIRE(groupby.group("allen").at(0)->length() == 4);
        REQUIRE(groupby.group("victor").at(i)->length() == 3);
        REQUIRE(groupby.group("hannah").at(i)->length() == 3);
    }

    REQUIRE(groupby.unique()->length() == 3);
    REQUIRE(pd::Scalar(groupby.unique()->GetScalar(0)).as<std::string>() == "allen");
    REQUIRE(pd::Scalar(groupby.unique()->GetScalar(1)).as<std::string>() == "victor");
    REQUIRE(pd::Scalar(groupby.unique()->GetScalar(2)).as<std::string>() == "hannah");

    auto allen_group = groupby.group("allen");

    REQUIRE(pd::Scalar(allen_group[0]->GetScalar(0)).as<std::string>() == "allen");
    REQUIRE(pd::Scalar(allen_group[0]->GetScalar(1)).as<std::string>() == "allen");
    REQUIRE(pd::Scalar(allen_group[0]->GetScalar(2)).as<std::string>() == "allen");
    REQUIRE(pd::Scalar(allen_group[0]->GetScalar(3)).as<std::string>() == "allen");
    REQUIRE(pd::Scalar(allen_group[1]->GetScalar(0)).as<int>() == 16);
    REQUIRE(pd::Scalar(allen_group[1]->GetScalar(1)).as<int>() == 20);
    REQUIRE(pd::Scalar(allen_group[1]->GetScalar(2)).as<int>() == 15);
    REQUIRE(pd::Scalar(allen_group[1]->GetScalar(3)).as<int>() == 45);

    auto hannah_group = groupby.group("hannah");
    REQUIRE(pd::Scalar(hannah_group[0]->GetScalar(0)).as<std::string>() == "hannah");
    REQUIRE(pd::Scalar(hannah_group[0]->GetScalar(1)).as<std::string>() == "hannah");
    REQUIRE(pd::Scalar(hannah_group[0]->GetScalar(2)).as<std::string>() == "hannah");
    REQUIRE(pd::Scalar(hannah_group[1]->GetScalar(0)).as<int>() == 10);
    REQUIRE(pd::Scalar(hannah_group[1]->GetScalar(1)).as<int>() == 40);
    REQUIRE(pd::Scalar(hannah_group[1]->GetScalar(2)).as<int>() == 35);

    auto victor_group = groupby.group("victor");
    REQUIRE(pd::Scalar(victor_group[0]->GetScalar(0)).as<std::string>() == "victor");
    REQUIRE(pd::Scalar(victor_group[0]->GetScalar(1)).as<std::string>() == "victor");
    REQUIRE(pd::Scalar(victor_group[0]->GetScalar(2)).as<std::string>() == "victor");
    REQUIRE(pd::Scalar(victor_group[1]->GetScalar(0)).as<int>() == 10);
    REQUIRE(pd::Scalar(victor_group[1]->GetScalar(1)).as<int>() == 30);
    REQUIRE(pd::Scalar(victor_group[1]->GetScalar(2)).as<int>() == 25);
}

TEST_CASE("makeGroups() works on dataframe with more than two column keys", "[makeGroups]")
{
    // Create a DataFrame with multiple column keys
    auto df = pd::DataFrame{ pd::range(0L, 10L),
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
                             std::pair{ "age"s, std::vector{ 16, 10, 10, 20, 30, 40, 15, 25, 35, 45 } },
                             std::pair{ "height"s, std::vector{ 9, 9, 9, 9, 9, 8, 8, 8, 8, 8 } } };

#define MALE 0
#define FEMALE 1
    pd::GroupBy groupby("gender", df);

    SECTION("groupSize() is valid")
    {
        REQUIRE(groupby.groupSize() == 2);
        REQUIRE(groupby.group("male").size() == 4);
        REQUIRE(groupby.group("female").size() == 4);

        for (int i = 0; i < 4; i++)
        {
            REQUIRE(groupby.group("male").at(i)->length() == 7);
            REQUIRE(groupby.group("female").at(i)->length() == 3);
        }

        REQUIRE(groupby.unique()->length() == 2);
    }

    SECTION("grouper selected correct unique_keys")
    {
        REQUIRE(pd::Scalar(groupby.unique()->GetScalar(0)).as<std::string>() == "male");
        REQUIRE(pd::Scalar(groupby.unique()->GetScalar(1)).as<std::string>() == "female");
    }


    SECTION("grouper created groups correctly")
    {
        auto male_group = groupby.group("male");

        REQUIRE(pd::Scalar(male_group[0]->GetScalar(0)).as<std::string>() == "allen");
        REQUIRE(pd::Scalar(male_group[0]->GetScalar(1)).as<std::string>() == "hannah");
        REQUIRE(pd::Scalar(male_group[0]->GetScalar(2)).as<std::string>() == "allen");
        REQUIRE(pd::Scalar(male_group[0]->GetScalar(3)).as<std::string>() == "hannah");
        REQUIRE(pd::Scalar(male_group[0]->GetScalar(4)).as<std::string>() == "allen");
        REQUIRE(pd::Scalar(male_group[0]->GetScalar(5)).as<std::string>() == "hannah");
        REQUIRE(pd::Scalar(male_group[0]->GetScalar(6)).as<std::string>() == "allen");

        for (int i = 0; i < 7; i++)
        {
            REQUIRE(pd::Scalar(male_group[1]->GetScalar(i)).as<std::string>() == "male");
        }

        REQUIRE(pd::Scalar(male_group[2]->GetScalar(0)).as<int>() == 16);
        REQUIRE(pd::Scalar(male_group[2]->GetScalar(1)).as<int>() == 10);
        REQUIRE(pd::Scalar(male_group[2]->GetScalar(2)).as<int>() == 20);
        REQUIRE(pd::Scalar(male_group[2]->GetScalar(3)).as<int>() == 40);
        REQUIRE(pd::Scalar(male_group[2]->GetScalar(4)).as<int>() == 15);
        REQUIRE(pd::Scalar(male_group[2]->GetScalar(5)).as<int>() == 35);
        REQUIRE(pd::Scalar(male_group[2]->GetScalar(6)).as<int>() == 45);

        auto female_group = groupby.group("female");
        REQUIRE(pd::Scalar(female_group[0]->GetScalar(0)).as<std::string>() == "victor");
        REQUIRE(pd::Scalar(female_group[0]->GetScalar(1)).as<std::string>() == "victor");
        REQUIRE(pd::Scalar(female_group[0]->GetScalar(2)).as<std::string>() == "victor");

        for (int i = 0; i < 3; i++)
        {
            REQUIRE(pd::Scalar(female_group[1]->GetScalar(i)).as<std::string>() == "female");
        }

        REQUIRE(pd::Scalar(female_group[2]->GetScalar(0)).as<int>() == 10);
        REQUIRE(pd::Scalar(female_group[2]->GetScalar(1)).as<int>() == 30);
        REQUIRE(pd::Scalar(female_group[2]->GetScalar(2)).as<int>() == 25);
    }

    SECTION("grouper computes mean of each groups and merge")
    {
        auto age_mean = pd::ReturnOrThrowOnFailure(groupby.mean("age"));
        REQUIRE(age_mean[0].as<double>() == Catch::Approx(25.857).epsilon(1e-3));
        REQUIRE(age_mean[1].as<double>() == Catch::Approx(21.667).epsilon(1e-3));

        auto age_height_mean = pd::ReturnOrThrowOnFailure(groupby.mean({ "age"s, "height"s }));
        INFO(age_height_mean);
        REQUIRE(age_height_mean["age"][0].as<double>() == 25.857142857142858);
        REQUIRE(age_height_mean["age"][1].as<double>() == 21.666666666666668);

        REQUIRE(age_height_mean["height"][0].as<double>() == 8.428571428571429);
        REQUIRE(age_height_mean["height"][1].as<double>() == 8.666666666666666);
    }

    SECTION("grouper computes min_max of each groups and merge")
    {
        pd::GroupBy groupby1("gender", df);
        auto age_max = pd::ReturnOrThrowOnFailure(groupby1.min_max("age"));

        INFO(age_max);
        REQUIRE(age_max["min"][MALE].as<int32_t>() == 10);
        REQUIRE(age_max["min"][FEMALE].as<int32_t>() == 10);

        REQUIRE(age_max["max"][MALE].as<int32_t>() == 45);
        REQUIRE(age_max["max"][FEMALE].as<int32_t>() == 30);

        auto age_height_max = pd::ReturnOrThrowOnFailure(groupby.min_max({ "age"s, "height"s }));
        INFO(age_height_max);
        REQUIRE(age_height_max["age_min"][MALE].as<int32_t>() == 10);
        REQUIRE(age_height_max["age_min"][FEMALE].as<int32_t>() == 10);

        REQUIRE(age_height_max["age_max"][MALE].as<int32_t>() == 45);
        REQUIRE(age_height_max["age_max"][FEMALE].as<int32_t>() == 30);

        REQUIRE(age_height_max["height_min"][MALE].as<int32_t>() == 8);
        REQUIRE(age_height_max["height_min"][FEMALE].as<int32_t>() == 8);

        REQUIRE(age_height_max["height_max"][MALE].as<int32_t>() == 9);
        REQUIRE(age_height_max["height_max"][FEMALE].as<int32_t>() == 9);
    }

    SECTION("grouper computes max of each groups and merge")
    {
        auto age_max = pd::ReturnOrThrowOnFailure(groupby.max("age"));
        REQUIRE(age_max[0].as<int32_t>() == 45);
        REQUIRE(age_max[1].as<int32_t>() == 30);

        auto age_height_max = pd::ReturnOrThrowOnFailure(groupby.max({ "age"s, "height"s }));

        REQUIRE(age_height_max["age"][0].as<int32_t>() == 45);
        REQUIRE(age_height_max["age"][1].as<int32_t>() == 30);

        REQUIRE(age_height_max["height"][0].as<int32_t>() == 9);
        REQUIRE(age_height_max["height"][1].as<int32_t>() == 9);
    }

    SECTION("grouper computes min of each groups and merge")
    {
        auto age_min = pd::ReturnOrThrowOnFailure(groupby.min("age"));
        REQUIRE(age_min[0].as<int32_t>() == 10);
        REQUIRE(age_min[1].as<int32_t>() == 10);
    }

    SECTION("grouper computes sum of each groups and merge")
    {
        auto age_sum = pd::ReturnOrThrowOnFailure(groupby.sum("age"));
        REQUIRE(age_sum[0].as<int64_t>() == 181);
        REQUIRE(age_sum[1].as<int64_t>() == 65);

        auto age_height_sum = pd::ReturnOrThrowOnFailure(groupby.sum({ "age"s, "height"s }));

        REQUIRE(age_height_sum["age"][0].as<int64_t>() == 181);
        REQUIRE(age_height_sum["age"][1].as<int64_t>() == 65);

        REQUIRE(age_height_sum["height"][0].as<int64_t>() == 59);
        REQUIRE(age_height_sum["height"][1].as<int64_t>() == 26);
    }

    SECTION("grouper computes count of each groups and merge")
    {
        auto age_count = pd::ReturnOrThrowOnFailure(groupby.count("age"));
        REQUIRE(age_count[0].as<::int64_t>() == 7);
        REQUIRE(age_count[1].as<::int64_t>() == 3);
    }

    SECTION("BarData Resample")
    {
        pd::DataFrame bardata{ arrow::DateTimeArray::Make({ time_from_string("2002-01-01 09:30:05.030376354"),
                                                            time_from_string("2002-01-01 09:31:05.030376354"),
                                                            time_from_string("2002-01-02 09:30:05.030376354"),
                                                            time_from_string("2002-01-02 16:30:05.030376354"),
                                                            time_from_string("2002-01-05 09:30:05.030376354") }),
                               std::pair("high"s, std::vector<float>{ 11.1, 20.2, 21., 15, 20 }),
                               std::pair("low"s, std::vector<float>{ 9.1, 9.2, 10., 5, 10 }),
                               std::pair("close"s, std::vector<float>{ 10.1, 15.2, 20., 15, 15 }),
                               std::pair("open"s, std::vector<float>{ 10, 20.2, 10., 15, 10 }),
                               std::pair("volume"s, std::vector<uint64_t>{ 100, 200, 210, 1, 2 }) };

        auto day_info = bardata.index().dt().day();
        pd::DataFrame new_df = pd::ReturnOrThrowOnFailure(bardata.array()->AddColumn(5, "day", day_info.array()));

        auto grouper = new_df.group_by("day");

        REQUIRE(grouper.unique()->length() == 3);

        REQUIRE(grouper.group(1l).size() == 6);
        REQUIRE(grouper.group(1l).at(0)->length() == 2);

        REQUIRE(grouper.group(2l).size() == 6);
        REQUIRE(grouper.group(2l).at(0)->length() == 2);

        REQUIRE(grouper.group(5l).size() == 6);
        REQUIRE(grouper.group(5l).at(0)->length() == 1);

        auto open = grouper.first("open");
        auto close = grouper.last("close");
        auto high = grouper.max("high");
        auto low = grouper.min("low");
        auto volume = grouper.sum("volume");

        new_df = pd::DataFrame{
            arrow::schema(arrow::FieldVector{ arrow::field("high", arrow::float32()),
                                              arrow::field("low", arrow::float32()),
                                              arrow::field("close", arrow::float32()),
                                              arrow::field("open", arrow::float32()),
                                              arrow::field("volume", arrow::uint64()),
                                              arrow::field("date", arrow::timestamp(arrow::TimeUnit::NANO)) }),
            3,
            { high->array(),
              low->array(),
              close->array(),
              open->array(),
              volume->array(),
              arrow::DateArray::Make({ date(2002, 01, 01), date(2002, 01, 02), date(2002, 01, 05) }) }
        };
        new_df = new_df.setIndex("date");
        std::cout << bardata << "\n";
        std::cout << new_df << "\n";
    }
}

TEST_CASE("Test apply method with Series input", "[GroupBy]")
{
    auto df =
        pd::DataFrame(std::map<std::string, std::vector<::int32_t>>{ { "a", { 1, 1, 3, 1, 1, 1, 3, 8, 2, 2 } },
                                                                     { "b", { 10, 9, 8, 7, 6, 5, 4, 3, 2, 1 } } });

    auto groupby = df.group_by("a"s);
    REQUIRE(groupby.groupSize() == 4);

    auto summation = [](pd::Series const& s) -> std::shared_ptr<arrow::Scalar> { return s.sum().scalar; };

    // Apply function to each group
    pd::DataFrame result{ nullptr };

    SECTION("Group synchronously")
    {
        // Apply function to each group
        result = pd::ReturnOrThrowOnFailure(groupby.apply(summation));
    }

// TODO: FIX
//    SECTION("Group ASynchronously")
//    {
//        // Apply function to each group
//        result = pd::ReturnOrThrowOnFailure(groupby.apply_async(summation));
//    }

    // Check that the result DataFrame has the correct number of rows and columns
    REQUIRE(result.num_rows() == groupby.groupSize());
    REQUIRE(result.num_columns() == 2);

    // Check that the values in the result DataFrame are as expected
    REQUIRE(result["a"].values<::int64_t>() == std::vector<int64_t>{ 5, 6, 8, 4 });
    REQUIRE(result["b"].values<int64_t>() == std::vector<int64_t>{ 37, 12, 3, 3 });
}

TEST_CASE("Test apply method with DataFrame input", "[GroupBy]")
{
    auto df =
        pd::DataFrame(std::map<std::string, std::vector<::int32_t>>{ { "a", { 1, 1, 3, 1, 1, 1, 3, 8, 2, 2 } },
                                                                     { "b", { 10, 9, 8, 7, 6, 5, 4, 3, 2, 1 } } });

    auto groupby = df.group_by("a"s);
    REQUIRE(groupby.groupSize() == 4);

    auto summation = [](pd::DataFrame const& s) -> std::shared_ptr<arrow::Scalar> { return s.sum().scalar; };

    pd::Series result{ std::vector<::int64_t>{} };

    SECTION("Group synchronously")
    {
        // Apply function to each group
        result = pd::ReturnOrThrowOnFailure(groupby.apply(summation));
    }

    SECTION("Group ASynchronously")
    {
        // Apply function to each group
        result = pd::ReturnOrThrowOnFailure(groupby.apply_async(summation));
    }

    // Check that the result DataFrame has the correct number of rows and columns
    REQUIRE(result.size() == groupby.groupSize());

    // Check that the values in the result DataFrame are as expected
    REQUIRE(result.values<::int64_t>() == std::vector<int64_t>{ 42, 18, 11, 7 });
}

// TODO: FIX
//TEST_CASE("generate_bins", "[Core]")
//{
//    auto value = pd::range(1L, 7L);
//    std::vector<std::tuple<std::vector<::int64_t>, bool, std::vector<::int64_t>>> params{
//        { { 0, 3, 6, 9 }, false, { 2, 5, 6 } },
//        { { 0, 3, 6, 9 }, true, { 3, 6, 6 } },
//        { { 0, 3, 6 }, false, { 2, 5 } },
//        { { 0, 3, 6 }, true, { 3, 6 } }
//    };
//
//    for (auto const& [binner, closed_right, expected] : params)
//    {
//        DYNAMIC_SECTION("binner_size" << binner.size() << " closed_right=" << closed_right)
//        {
//            REQUIRE(pd::generate_bins_dt64(value, arrow::ArrayT<std::int64_t>::Make(binner), closed_right) == expected);
//        }
//    }
//}

TEST_CASE("Test groupinfo downsampling", "[Resample]")
{
    std::vector<ptime> index{ time_from_string("2000-01-01 00:00:00.000000000"),
                              time_from_string("2000-01-01 00:03:00.000000000"),
                              time_from_string("2000-01-01 00:06:00.000000000") };

    pd::GroupInfo info{ std::vector<::int64_t>{ 3, 6, 9 }, arrow::DateTimeArray::Make(index) };
    auto groups = info.downsample();

    REQUIRE(groups.size() == 9);
    REQUIRE(groups[0] == pd::fromPTime(index[0]));
    REQUIRE(groups[1] == pd::fromPTime(index[0]));
    REQUIRE(groups[2] == pd::fromPTime(index[0]));
    REQUIRE(groups[3] == pd::fromPTime(index[1]));
    REQUIRE(groups[4] == pd::fromPTime(index[1]));
    REQUIRE(groups[5] == pd::fromPTime(index[1]));
    REQUIRE(groups[6] == pd::fromPTime(index[2]));
    REQUIRE(groups[7] == pd::fromPTime(index[2]));
    REQUIRE(groups[8] == pd::fromPTime(index[2]));
    REQUIRE_FALSE(info.upsampling());
}

TEST_CASE("Test groupinfo upsampling", "[Resample]")
{
    std::vector<ptime> index{
        time_from_string("2000-01-01 00:00:00.000000000"), time_from_string("2000-01-01 00:00:30.000000000"),
        time_from_string("2000-01-01 00:01:00.000000000"), time_from_string("2000-01-01 00:01:30.000000000"),
        time_from_string("2000-01-01 00:02:00.000000000"), time_from_string("2000-01-01 00:02:30.000000000"),
        time_from_string("2000-01-01 00:03:00.000000000"), time_from_string("2000-01-01 00:03:30.000000000"),
        time_from_string("2000-01-01 00:04:00.000000000"), time_from_string("2000-01-01 00:04:30.000000000"),
        time_from_string("2000-01-01 00:05:00.000000000"), time_from_string("2000-01-01 00:05:30.000000000"),
        time_from_string("2000-01-01 00:06:00.000000000"), time_from_string("2000-01-01 00:06:30.000000000"),
        time_from_string("2000-01-01 00:07:00.000000000"), time_from_string("2000-01-01 00:07:30.000000000"),
        time_from_string("2000-01-01 00:08:00.000000000")
    };

    pd::GroupInfo info{ std::vector<::int64_t>{ 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8, 9 },
                        arrow::DateTimeArray::Make(index) };
    REQUIRE(info.upsampling());
}

TEST_CASE("Test reindex vs reindex_async benchmark", "[reindex]")
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
        std::pair{ "toy"s, std::vector{ ""s, "Batman"s, "Bullwhip"s } },
        std::pair{ "born"s, std::vector{ ""s, "1940-04-25"s, ""s } },
    };

    df["born"] = df["born"].to_datetime();

    df = df.drop_na();

    INFO(df);

    REQUIRE(df.shape() == pd::DataFrame::Shape{ 1, 3 });

    REQUIRE(df.at(0, 0).scalar->ToString() == "Batman");
    REQUIRE(df.at(0, 1).scalar->ToString() == "Batman");
    REQUIRE(df.at(0, 2).scalar->ToString() == "1940-04-25");
    REQUIRE(df.index()[0] == 4L);
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