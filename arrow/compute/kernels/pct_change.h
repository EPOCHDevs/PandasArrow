#pragma once
//
// Created by dewe on 1/16/23.
//
#include "arrow/compute/api.h"
#include "shift.h"


namespace arrow::compute {

inline arrow::Result<arrow::Datum> PctChange(const std::shared_ptr<arrow::Array>& input, int periods)
{
    if (periods > input->length())
    {
        return arrow::Status::Invalid("Periods cannot be greater than the length of the array");
    }

    // Shift the input array by the number of periods
    ARROW_ASSIGN_OR_RAISE(
        auto shifted_array,
        Shift(input, ShiftOptions{ periods, arrow::MakeNullScalar(input->type()) }));

    // Divide the shifted array by the original array and subtract 1.0
    ARROW_ASSIGN_OR_RAISE(auto divided_array, Divide(input, shifted_array));
    return Subtract(divided_array, MakeScalar(1.0));
}

} // namespace arrow::compute