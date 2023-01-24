#pragma once
//
// Created by dewe on 1/2/23.
//

#include <tbb/parallel_for.h>
#include <arrow/compute/exec/test_util.h>

#include <utility>
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
    GroupBy(std::string key, pd::DataFrame  df) : df(std::move(df))
    {
        auto result = makeGroups(std::move(key));
        if (not result.ok())
        {
            throw std::runtime_error(result.ToString());
        }
    }

    inline size_t groupSize() const
    {
        return groups.size();
    }

    template<class T>
        requires(not std::same_as<T, std::shared_ptr<arrow::Scalar>>)
    inline arrow::ArrayVector group(T&& value) const
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
        return uniqueKeys;
    }

    inline std::shared_ptr<arrow::Scalar> GetKeyByIndex(::int64_t i) const
    {
        return ReturnOrThrowOnFailure(uniqueKeys->GetScalar(i));
    }

    arrow::Result<pd::Series> apply(
        std::function<std::shared_ptr<arrow::Scalar>(DataFrame const&)> fn);

    arrow::Result<pd::DataFrame> apply(
        std::function<std::shared_ptr<arrow::Scalar>(Series const&)> fn);

    arrow::Result<pd::Series> apply_async(
        std::function<std::shared_ptr<arrow::Scalar>(DataFrame const&)> fn);

    arrow::Result<pd::DataFrame> apply_async(
        std::function<std::shared_ptr<arrow::Scalar>(Series const&)> fn);

    arrow::Result<pd::DataFrame> mean(std::vector<std::string> const& args);
    arrow::Result<pd::Series> mean(std::string const& arg);

    arrow::Result<pd::DataFrame> all(std::vector<std::string> const& args);
    arrow::Result<pd::Series> all(std::string const& arg);

    arrow::Result<pd::DataFrame> any(std::vector<std::string> const& args);
    arrow::Result<pd::Series> any(std::string const& arg);

    arrow::Result<pd::DataFrame> approximate_median(
        std::vector<std::string> const& args);
    arrow::Result<pd::Series> approximate_median(std::string const& arg);

    arrow::Result<pd::DataFrame> count(std::vector<std::string> const& args);
    arrow::Result<pd::Series> count(std::string const& arg);

    arrow::Result<pd::DataFrame> count_distinct(
        std::vector<std::string> const& args);
    arrow::Result<pd::Series> count_distinct(std::string const& arg);

    arrow::Result<pd::DataFrame> first(std::vector<std::string> const& args);

    arrow::Result<pd::Series> first(std::string const& arg);

    arrow::Result<pd::DataFrame> last(std::vector<std::string> const& args);

    arrow::Result<pd::Series> last(std::string const& arg);

    arrow::Result<pd::DataFrame> max(std::vector<std::string> const& args);
    arrow::Result<pd::Series> max(std::string const& arg);

    arrow::Result<pd::DataFrame> min(std::vector<std::string> const& args);
    arrow::Result<pd::Series> min(std::string const& arg);

    arrow::Result<pd::DataFrame> min_max(std::vector<std::string> const& args);
    arrow::Result<pd::DataFrame> min_max(std::string const& arg);

    arrow::Result<pd::DataFrame> product(std::vector<std::string> const& args);
    arrow::Result<pd::Series> product(std::string const& arg);

    arrow::Result<pd::DataFrame> quantile(
        std::vector<std::string> const& args,
        std::vector<double> const& q);
    arrow::Result<pd::Series> quantile(std::string const& arg, double q);

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

protected:
    inline const DataFrame& getDF() const
    {
        return df;
    }

private:
    GroupMap groups;
    DataFrame df;
    std::unordered_map<
        std::shared_ptr<arrow::Scalar>,
        std::shared_ptr<arrow::Array>,
        pd::HashScalar,
        pd::HashScalar>
        indexGroups;
    std::shared_ptr<arrow::Array> uniqueKeys;

    template<typename OptionT, typename... Args>
    static OptionT convertToArrowFunctionOption(Args const&... args)
    {
        return OptionT(args...);
    }

    template<typename OptionT, typename Args>
    static std::vector<OptionT> convertToArrowFunctionOptions(
        std::vector<Args> const& args)
    {
        std::vector<OptionT> options(args.size());
        std::ranges::transform(
            args,
            options.begin(),
            convertToArrowFunctionOption<OptionT, Args>);
        return options;
    }

    arrow::Result<std::shared_ptr<arrow::ArrayData>> buildData(
        arrow::ScalarVector const& arg)
    {
        std::shared_ptr<arrow::ArrayBuilder> builder;
        if (not arg.empty())
        {
            builder = arrow::MakeBuilder(arg.back()->type).MoveValueUnsafe();
            ARROW_RETURN_NOT_OK(builder->AppendScalars(arg));
        }

        std::shared_ptr<arrow::ArrayData> data;
        ARROW_RETURN_NOT_OK(builder->FinishInternal(&data));
        return data;
    }

    static arrow::Result<std::shared_ptr<arrow::Array>> buildArray(
        arrow::ScalarVector const& arg)
    {
        std::shared_ptr<arrow::ArrayBuilder> builder;
        if (not arg.empty())
        {
            builder = arrow::MakeBuilder(arg.back()->type).MoveValueUnsafe();
            RETURN_NOT_OK(builder->AppendScalars(arg));
        }

        std::shared_ptr<arrow::Array> data;
        RETURN_NOT_OK(builder->Finish(&data));
        return data;
    }

    static inline auto defaultOpt =
        std::shared_ptr<arrow::compute::FunctionOptions>();

    /// takes in a grouper (an object that groups rows in a DataFrame based
    /// on certain criteria), a groupings array (which specifies the groups
    /// that the rows are grouped into), and a column array (which contains
    /// the data of a specific column in the DataFrame). It uses the grouper
    /// to group the rows in the column array based on the groupings, and
    /// stores the grouped data in the groups attribute.
    arrow::Status processEach(
        std::unique_ptr<arrow::compute::Grouper> const& grouper,
        std::shared_ptr<arrow::ListArray> const& groupings,
        std::shared_ptr<arrow::Array> const& column);

    /// The processIndex() function takes in the same inputs as processEach(),
    /// but it is used to group the rows in the index column of the DataFrame,
    /// rather than a specific column.
    arrow::Status processIndex(
        std::unique_ptr<arrow::compute::Grouper> const& grouper,
        std::shared_ptr<arrow::ListArray> const& groupings);

    arrow::Status makeGroups(std::string const& keyInStringFormat);

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

#define RESAMPLE_GROUP_BY_FUNCTION(name) \
    arrow::Result<pd::DataFrame> name() \
{ \
    return GroupBy::name(getDF().columnNames())->setIndex(this->unique()); \
}

struct Resampler : protected GroupBy
{
    Resampler(DataFrame const& _df)
        : GroupBy("__resampler_idx__", _df )
    {
    }

    friend std::ostream& operator<<(
        std::ostream& os,
        Resampler const& resampler)
    {
        os << resampler.getDF() << "\n";
        return os;
    }

    inline auto index() const
    {
        return this->unique();
    }

    inline auto data() const
    {
        return this->getDF();
    }

    RESAMPLE_GROUP_BY_FUNCTION(mean)
    RESAMPLE_GROUP_BY_FUNCTION(all)
    RESAMPLE_GROUP_BY_FUNCTION(any)
    RESAMPLE_GROUP_BY_FUNCTION(approximate_median)
    RESAMPLE_GROUP_BY_FUNCTION(count)
    RESAMPLE_GROUP_BY_FUNCTION(count_distinct)
    RESAMPLE_GROUP_BY_FUNCTION(max)
    RESAMPLE_GROUP_BY_FUNCTION(min)
    RESAMPLE_GROUP_BY_FUNCTION(min_max)
    RESAMPLE_GROUP_BY_FUNCTION(product)
    RESAMPLE_GROUP_BY_FUNCTION(mode)
    RESAMPLE_GROUP_BY_FUNCTION(sum)
    RESAMPLE_GROUP_BY_FUNCTION(stddev)
    RESAMPLE_GROUP_BY_FUNCTION(variance)
    RESAMPLE_GROUP_BY_FUNCTION(tdigest)

    using GroupBy::apply_async;
    using GroupBy::apply;

};

}