#pragma once
//
// Created by dewe on 12/29/22.
//


#include "arrow/compute/kernels/autocorr.h"
#include "arrow/compute/kernels/corr.h"
#include "arrow/compute/kernels/pct_change.h"
#include "arrow/compute/kernels/shift.h"
#include "concat.h"
#include "core.h"
#include "datetimelike.h"
#include "group_by.h"
#include "resample.h"
#include "stringlike.h"

namespace arrow::compute {

struct CustomKernelsRegistryImpl
{
    CustomKernelsRegistryImpl()
    {
        arrow::compute::internal::RegisterScalarAggregateCovariance(GetFunctionRegistry());
        arrow::compute::internal::RegisterScalarAggregateCorrelation(GetFunctionRegistry());
        arrow::compute::internal::MakeVectorShiftFunction(GetFunctionRegistry());
    }
};

struct CustomKernelsRegistry
{
    inline static CustomKernelsRegistryImpl impl;
};

const CustomKernelsRegistry __REGISTER__CUSTOM__KERNELS__;

} // namespace arrow::compute

namespace pd {
template<class V>
using TableLike = std::map<std::string, std::vector<V>>;
}