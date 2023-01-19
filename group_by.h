#pragma once
//
// Created by dewe on 1/2/23.
//

#include <tbb/parallel_for.h>
#include <arrow/compute/exec/test_util.h>
#include "unordered_map"
#include "string"
#include "arrow/compute/exec/exec_plan.h"
#include "arrow/compute/api_aggregate.h"
#include "arrow/compute/api_vector.h"
#include "arrow/compute/row/grouper.h"
#include "dataframe.h"
#include "series.h"

using GroupMap = std::unordered_map<std::shared_ptr<arrow::Scalar>,
    arrow::ArrayVector, pd::HashScalar, pd::HashScalar>;

namespace pd {

struct GroupBy
{
    std::string keyStr;
    const DataFrame& df;
    GroupBy(std::string key, const pd::DataFrame& df)
        : keyStr(std::move(key)), df(df)
    {
        auto result = makeGroups();
        if (not result.ok())
        {
            throw std::runtime_error(result.ToString());
        }
    }

    inline ::int64_t groupSize() const
    {
        return groups.size();
    }

    template<class T> requires (not std::same_as<T, std::shared_ptr<arrow::Scalar>>)
    inline arrow::ArrayVector group(T && value) const
    {
        try
        {
            return groups.at(arrow::MakeScalar(std::forward<T>(value)));
        }
        catch (std::out_of_range const& exception)
        {
            std::cout << value << " is an invalid key\n";
            throw;
        }
    }

    inline std::shared_ptr<arrow::Array> unique() const
    {
        return unique_value;
    }

//    arrow::Result<pd::DataFrame> apply(
//        std::function<std::shared_ptr<arrow::Scalar>(pd::DataFrame const&)> fn,
//        std::vector<std::string> const& args);

//    arrow::Result<pd::DataFrame> apply(
//        std::function<std::shared_ptr<arrow::Scalar>(pd::DataFrame const&)> fn);

    arrow::Result<pd::DataFrame> mean(std::vector<std::string> const& args);
    arrow::Result<pd::Series> mean(std::string const& arg);

    arrow::Result<pd::DataFrame> all(std::vector<std::string> const& args);
    arrow::Result<pd::Series> all(std::string const& arg);

    arrow::Result<pd::DataFrame> any(std::vector<std::string> const& args);
    arrow::Result<pd::Series> any(std::string const& arg);

    arrow::Result<pd::DataFrame> approximate_median(std::vector<std::string> const& args);
    arrow::Result<pd::Series> approximate_median(std::string const& arg);

    arrow::Result<pd::DataFrame> count(std::vector<std::string> const& args);
    arrow::Result<pd::Series> count(std::string const& arg);

    arrow::Result<pd::DataFrame> count_distinct(std::vector<std::string> const& args);
    arrow::Result<pd::Series> count_distinct(std::string const& arg);

    arrow::Result<pd::DataFrame> index(std::vector<std::string> const& args);
    arrow::Result<pd::Series> index(std::string const& arg);

    arrow::Result<pd::DataFrame> max(std::vector<std::string> const& args);
    arrow::Result<pd::Series> max(std::string const& arg);

    arrow::Result<pd::DataFrame> min(std::vector<std::string> const& args);
    arrow::Result<pd::Series> min(std::string const& arg);

    arrow::Result<pd::DataFrame> min_max(std::vector<std::string> const& args);
    arrow::Result<pd::Series> min_max(std::string const& arg);

    arrow::Result<pd::DataFrame> product(std::vector<std::string> const& args);
    arrow::Result<pd::Series> product(std::string const& arg);

    arrow::Result<pd::DataFrame> quantile(std::vector<std::string> const& args);
    arrow::Result<pd::Series> quantile(std::string const& arg);

    arrow::Result<pd::DataFrame> mode(std::vector<std::string> const& args);
    arrow::Result<pd::Series> mode(std::string const& arg);

    arrow::Result<pd::DataFrame> sum(std::vector<std::string> const& args);
    arrow::Result<pd::Series> sum(std::string const& arg);

    arrow::Result<pd::DataFrame> stddev(std::vector<std::string> const& args);
    arrow::Result<pd::Series> stddev(std::string const& arg);

    arrow::Result<pd::DataFrame> variance(std::vector<std::string> const& args);
    arrow::Result<pd::Series> variance(std::string const& arg);

    arrow::Result<pd::DataFrame> tdigest(std::vector<std::string> const& args);
    arrow::Result<pd::Series> tdigest(std::string const& arg);

private:
    GroupMap groups;
    std::unordered_map<
        std::shared_ptr<arrow::Scalar>,
        std::shared_ptr<arrow::Array>,
        pd::HashScalar,
        pd::HashScalar>
        indexChunk;
    std::shared_ptr<arrow::Array> unique_value;

    static inline auto defaultOpt =
        std::make_shared<arrow::compute::ScalarAggregateOptions>();

    arrow::Status processEach(
        std::unique_ptr<arrow::compute::Grouper> const& grouper,
        std::shared_ptr<arrow::ListArray> const& groupings,
        std::shared_ptr<arrow::Array> const& column);

    arrow::Status processIndex(
        std::unique_ptr<arrow::compute::Grouper> const& grouper,
        std::shared_ptr<arrow::ListArray> const& groupings);

    arrow::Status makeGroups();

    arrow::FieldVector fieldVectors(
        std::vector<std::string> const& args,
        std::shared_ptr<arrow::Schema> const& schema)
    {
        arrow::FieldVector fv(args.size());
        std::ranges::transform(
            args,
            fv.begin(),
            [&](auto const& arg) { return schema->GetFieldByName(arg); });
        return fv;
    }
};

}