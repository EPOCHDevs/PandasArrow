//
// Created by dewe on 2/8/24.
//

#include "cudf_utils.h"


namespace cudf {

    bool operator==(std::vector<cudf::column_metadata> const &a, std::vector<cudf::column_metadata> const &b) {
        std::vector<bool> truth;
        truth.reserve(a.size());

        std::transform(
                a.begin(),
                a.end(),
                b.begin(),
                std::back_inserter(truth),
                [](auto const &a, auto const &b) { return a == b; });

        return std::ranges::all_of(truth, [](bool r) { return r; });
    }

    template<typename OutputType>
    bool complete_reduce(column_view const &columnView, std::unique_ptr<reduce_aggregation> const &agg) {
        std::unique_ptr<cudf::scalar> result = reduce(columnView,
                                                      *agg,
                                                      cudf::data_type(cudf::type_to_id<OutputType>()));
        ASSERT_SCALAR_IS_VALID(result);
        return GetNumericValue<bool>(*result);
    }

    bool any(column_view const &columnView) {
        return complete_reduce<bool>(columnView, make_any_aggregation<reduce_aggregation>());
    }

    bool all(column_view const &columnView) {
        return complete_reduce<bool>(columnView, make_all_aggregation<reduce_aggregation>());
    }
}