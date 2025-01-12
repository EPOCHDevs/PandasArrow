//
// Created by adesola on 1/11/25.
//
#include "pandas_arrow.h"
#include "catch.hpp"


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
        pd::DataFrame new_df{pd::ReturnOrThrowOnFailure(bardata.array()->AddColumn(5, "day", day_info.array()))};

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