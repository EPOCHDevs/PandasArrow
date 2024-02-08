#pragma once
//
// Created by dewe on 2/8/24.
//
#include <cudf/aggregation.hpp>
#include <cudf/reduction.hpp>
#include <cudf/binaryop.hpp>
#include "cudf/interop.hpp"


namespace cudf {

    inline bool operator==(column_metadata const &a, column_metadata const &b) {
        return a.name == b.name && a.children_meta.size() == b.children_meta.size();
    }

    bool operator==(std::vector<column_metadata> const &a, std::vector<column_metadata> const &b);

    bool any(column_view const &columnView);

    bool all(column_view const &columnView);

    template<class LHSType, class RHSType>
    std::unique_ptr<column> binary_operation(LHSType const &lhs,
                                             RHSType const &rhs,
                                             binary_operator binaryOperator)
                                             {
// TODO: Abstract out
        auto lhsType = lhs.type();
        auto rhsType = rhs.type();

        data_type outputType;
        if (is_fixed_point(lhsType) && is_fixed_point(rhsType)) {
            outputType = binary_operation_fixed_point_output_type(binaryOperator, lhsType, rhsType);
        } else if (lhsType == rhsType) {
            outputType = lhsType;
        } else {
            throw std::runtime_error("binary operation with non fixed_point output requires both type to equal");
        }
        return binary_operation(lhs, rhs, binaryOperator, outputType);
    }

    void AssertIsValid(auto &&ptr) {
        if (!ptr->is_valid()) {
            throw std::runtime_error("Scalar Is not Valid");
        }
    }

    template<class T>
    T GetNumericValue(cudf::scalar const &scalar) {
        return static_cast<const cudf::numeric_scalar<T> &>(scalar).value();
    }

#define ASSERT_IS_VALID(ptr, name) if (!ptr->is_valid()) throw std::runtime_error(std::string{name} + " is not Valid")
#define ASSERT_SCALAR_IS_VALID(ptr) ASSERT_IS_VALID(ptr, "Scalar")

}