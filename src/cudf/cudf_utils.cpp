//
// Created by dewe on 2/8/24.
//

#include "cudf_utils.h"


namespace cudf
{

bool operator==(std::vector<cudf::column_metadata> const& a, std::vector<cudf::column_metadata> const& b)
{
    std::vector<bool> truth;
    truth.reserve(a.size());

    std::transform(
        a.begin(),
        a.end(),
        b.begin(),
        std::back_inserter(truth),
        [](auto const& a, auto const& b) { return a == b; });

    return std::ranges::all_of(truth, [](bool r) { return r; });
}

template<typename OutputType>
OutputType complete_reduce(column_view const& columnView, std::unique_ptr<reduce_aggregation> const& agg);

template<>
std::unique_ptr<cudf::scalar> complete_reduce<std::unique_ptr<cudf::scalar>>(
    column_view const& columnView,
    std::unique_ptr<reduce_aggregation> const& agg)
{
    std::unique_ptr<cudf::scalar> result = reduce(columnView, *agg, columnView.type());
    ASSERT_SCALAR_IS_VALID(result);
    return result;
}

template<typename OutputType>
OutputType complete_reduce(column_view const& columnView, std::unique_ptr<reduce_aggregation> const& agg)
{
    std::unique_ptr<cudf::scalar> result = reduce(columnView, *agg, cudf::data_type(cudf::type_to_id<OutputType>()));
    ASSERT_SCALAR_IS_VALID(result);
    return GetNumericValue<OutputType>(*result);
}

bool any(column_view const& columnView)
{
    return complete_reduce<bool>(columnView, make_any_aggregation<reduce_aggregation>());
}

bool all(column_view const& columnView)
{
    return complete_reduce<bool>(columnView, make_all_aggregation<reduce_aggregation>());
}

double mean(column_view const& columnView)
{
    return complete_reduce<double>(columnView, make_mean_aggregation<reduce_aggregation>());
}

std::unique_ptr<cudf::scalar> min(column_view const& columnView)
{
    return complete_reduce<std::unique_ptr<cudf::scalar>>(columnView, make_min_aggregation<reduce_aggregation>());
}

std::unique_ptr<cudf::scalar> max(column_view const& columnView)
{
    return complete_reduce<std::unique_ptr<cudf::scalar>>(columnView, make_max_aggregation<reduce_aggregation>());
}

double stddev(column_view const& columnView)
{
    return complete_reduce<double>(columnView, make_std_aggregation<reduce_aggregation>());
}

double var(column_view const& columnView)
{
    return complete_reduce<double>(columnView, make_variance_aggregation<reduce_aggregation>());
}
}