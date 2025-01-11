//
// Created by dewe on 12/28/22.
//
#include <climits>
#include "scalar.h"
#include <iostream>
#include "filesystem"
#include "dataframe.h"
#include "series.h"


#define BINARY_OPERATOR_2(op, name) \
DataFrame Scalar::operator op(DataFrame const & df) const {\
    return BinaryImpl(df, this->scalar, #name);\
}\
Series Scalar::operator op(Series const & s) const {\
    return BinaryImpl(s, this->scalar, #name);\
}

namespace pd {
    Scalar::Scalar(std::shared_ptr<arrow::Scalar> &&value) : scalar(std::move(value)) {
    }

    DataFrame BinaryImpl(DataFrame const & df, std::shared_ptr<arrow::Scalar> const& scalar, std::string const& name) {
        auto chunkedArray = std::make_shared<arrow::ChunkedArray>(df.array()->columns());
        return df.Make(ReturnOrThrowOnFailure(
                arrow::compute::CallFunction(name, {scalar, chunkedArray})).chunks());
    }

    Series BinaryImpl(Series const & s, std::shared_ptr<arrow::Scalar> const& scalar, std::string const& name) {

        return Series{
                ReturnOrThrowOnFailure(
                        arrow::compute::CallFunction(name, {scalar, s.array()})).make_array(),
                s.indexArray()};
    }

    BINARY_OPERATOR_2(/, divide)
    BINARY_OPERATOR_2(+, add)
    BINARY_OPERATOR_2(-, subtract)
    BINARY_OPERATOR_2(*, multiply)

    BINARY_OPERATOR_2(|, bit_wise_or)
    BINARY_OPERATOR_2(&, bit_wise_and)
    BINARY_OPERATOR_2(^, bit_wise_xor)
    BINARY_OPERATOR_2(<<, shift_left)

    BINARY_OPERATOR_2(>>, shift_right)

    BINARY_OPERATOR_2(>, greater)

    BINARY_OPERATOR_2(>=, greater_equal)

    BINARY_OPERATOR_2(<, less)

    BINARY_OPERATOR_2(<=, less_equal)
    BINARY_OPERATOR_2(==, equal)
    BINARY_OPERATOR_2(!=, not_equal)

    BINARY_OPERATOR_2(&&, and)

    BINARY_OPERATOR_2(||, or)

} // namespace pd