#pragma once
//
// Created by dewe on 1/16/23.
//
#include "shift.h"
#include "arrow/compute/api.h"
#include "arrow/compute/api_aggregate.h"
#include "arrow/compute/kernels/cov.h"

namespace arrow::compute {

/// Compute the lag-N autocorrelation.
///
/// This method computes the Pearson correlation
/// between the Series and its shifted self.
inline arrow::Result<arrow::Datum> AutoCorr(
    const std::shared_ptr<arrow::Array>& input,
    int lag=1)
{
    if (lag > input->length())
    {
        return arrow::Status::Invalid(
            "Lag cannot be greater than the length of the array");
    }
    // Shift the input array by the lag value
    ARROW_ASSIGN_OR_RAISE(
        auto shifted_array,
        Shift(
            input,
            ShiftOptions{ lag, arrow::MakeNullScalar(input->type()) }));
    // Compute the covariance between the input array and the shifted array
    ARROW_ASSIGN_OR_RAISE(auto covariance, Covariance(input,
                                                      shifted_array.make_array()));
    // Compute the variance of the input array
    ARROW_ASSIGN_OR_RAISE(auto variance, Variance(input));
    // Divide the covariance by the variance to get the correlation
    ARROW_ASSIGN_OR_RAISE(auto correlation, Divide(covariance, variance));
    // Divide the correlation by the number of observations minus the lag to get the autocorrelation
    return Divide(correlation, MakeScalar(input->length() - 1 - lag));
}

}