//
// Created by dewe on 1/18/23.
//
#include "arrow/compute/row/grouper.h"
#include <mutex>

#include <thread>
#include <unordered_map>
#include <utility>
#include "arrow/compute/exec/aggregate.h"
#include "arrow/compute/exec_internal.h"
#include "arrow/compute/registry.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/logging.h"
#include "arrow/util/string.h"
#include "arrow/util/task_group.h"
#include "pandas_arrow.h"


int main()
{
    using namespace std::string_literals;
    auto rand = pd::random::RandomState(20);
    auto df = pd::DataFrame{
        pd::range(0, 16),
        std::pair{ "id"s, rand.choice<int>({ 0, 1, 4, 6, 4 }, 16) },
        std::pair{ "age"s, rand.randint(16, 10, 50) },
        std::pair{ "lover"s, rand.randint(16, 10, 50) },
        std::pair{ "sex"s, rand.randint(16, 10, 50) },
        std::pair{ "gt"s, rand.randint(16, 10, 50) },
        std::pair{ "k"s, rand.randint(16, 10, 50) },
        std::pair{ "height"s, rand.rand(16, 5, 10) },
    };
    std::cout << df << "\n";

    // Define the grouping key and columns to aggregate
    auto group_key = df["id"];
    std::vector<std::string> aggregate_columns = { "age", "height", "lover",
                                                   "sex", "gt",  "k" };

    // Define the aggregate functions to apply
    std::vector<arrow::compute::Aggregate> aggregates = {
        { "hash_mean",
          std::make_shared<arrow::compute::ScalarAggregateOptions>() },
        { "hash_mean",
          std::make_shared<arrow::compute::ScalarAggregateOptions>() },
        { "hash_mean",
          std::make_shared<arrow::compute::ScalarAggregateOptions>() },
        { "hash_mean",
          std::make_shared<arrow::compute::ScalarAggregateOptions>() },
        { "hash_mean",
          std::make_shared<arrow::compute::ScalarAggregateOptions>() },
        { "hash_mean",
          std::make_shared<arrow::compute::ScalarAggregateOptions>() }
    };

    auto start = std::chrono::high_resolution_clock ::now();
    for (int i = 0; i < 100; i++)
    {
        auto x = arrow::compute::internal::GroupBy(
                     { df["age"].array(),
                       df["height"].array(),
                       df["lover"].array(),
                       df["sex"].array(),
                       df["gt"].array(),
                       df["k"].array() },
                     { group_key.array() },
                     aggregates)
                     ->array_as<arrow::StructArray>();
    }
    auto end = std::chrono::high_resolution_clock ::now();

    std::cout << "regular: "
              << std::chrono::duration<double>(end - start).count() / 100.0
              << " s.\n";

    start = std::chrono::high_resolution_clock ::now();
    auto groups = df.group_by("id");
    arrow::Result<pd::DataFrame> r;
    for (int i = 0; i < 100; i++)
    {
        r = groups.mean(aggregate_columns);
    }
    end = std::chrono::high_resolution_clock ::now();

    std::cout << pd::ValidateAndReturn(std::move(r)) << "\n";
    std::cout << "optimized: "
              << std::chrono::duration<double>(end - start).count() / 100.0
              << " s.\n";

    auto result = pd::ValidateAndReturn(
        groups.apply([](pd::DataFrame const& df) { return df.sum().scalar; }));

    std::cout << result << "\n";

    return 0;
}
