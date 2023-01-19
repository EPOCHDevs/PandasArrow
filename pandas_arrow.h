#pragma once
//
// Created by dewe on 12/29/22.
//


#include "core.h"
#include "group_by.h"
#include "stringlike.h"
#include "datetimelike.h"
#include "arrow/compute/kernels/cumprod.h"
#include "arrow/compute/kernels/corr.h"
#include "arrow/compute/kernels/shift.h"
#include "arrow/compute/kernels/pct_change.h"
#include "arrow/compute/kernels/autocorr.h"

namespace arrow::compute {

struct CustomKernelsRegistryImpl
{
    CustomKernelsRegistryImpl()
    {
        arrow::compute::internal::RegisterVectorCumulativeProduct(
            GetFunctionRegistry());
        arrow::compute::internal::RegisterScalarAggregateCovariance(
            GetFunctionRegistry());
        arrow::compute::internal::RegisterScalarAggregateCorrelation(
            GetFunctionRegistry());
        arrow::compute::internal::MakeVectorShiftFunction(
            GetFunctionRegistry());
    }
};

struct CustomKernelsRegistry
{
    inline static CustomKernelsRegistryImpl impl;
};

const CustomKernelsRegistry __REGISTER__CUSTOM__KERNELS__;

}

namespace pd {
template<class V>
using TableLike = std::map<std::string, std::vector<V>>;
}