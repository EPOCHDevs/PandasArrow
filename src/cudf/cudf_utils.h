#pragma once
//
// Created by dewe on 2/8/24.
//
#include <cudf/aggregation.hpp>
#include <cudf/reduction.hpp>
#include <cudf/binaryop.hpp>
#include "cudf/interop.hpp"
#include "fmt/format.h"
#include "../dataframe.h"


namespace cudf
{

inline bool operator==(column_metadata const& a, column_metadata const& b)
{
    return a.name == b.name && a.children_meta.size() == b.children_meta.size();
}

bool operator==(std::vector<column_metadata> const& a, std::vector<column_metadata> const& b);

bool any(column_view const& columnView);

bool all(column_view const& columnView);

double mean(column_view const& columnView);

std::unique_ptr<cudf::scalar> min(column_view const& columnView);

std::unique_ptr<cudf::scalar> max(column_view const& columnView);

double stddev(column_view const& columnView);

double var(column_view const& columnView);

template<class LHSType, class RHSType>
std::unique_ptr<column> binary_operation(LHSType const& lhs, RHSType const& rhs, binary_operator binaryOperator)
{
    // TODO: Abstract out
    auto lhsType = lhs.type();
    auto rhsType = rhs.type();

    data_type outputType;
    if (is_fixed_point(lhsType) && is_fixed_point(rhsType))
    {
        outputType = binary_operation_fixed_point_output_type(binaryOperator, lhsType, rhsType);
    }
    else if (lhsType == rhsType)
    {
        outputType = lhsType;
    }
    else
    {
        throw std::runtime_error("binary operation with non fixed_point output requires both type to equal");
    }
    return binary_operation(lhs, rhs, binaryOperator, outputType);
}

inline void AssertIsValid(auto&& ptr)
{
    if (!ptr->is_valid())
    {
        throw std::runtime_error("Scalar Is not Valid");
    }
}

inline void AssertIsTrue(bool truth, const char* rep)
{
    if (!truth)
    {
        throw std::runtime_error(fmt::format("{} Failed.", rep));
    }
}

template<class T>
T GetNumericValue(cudf::scalar const& scalar)
{
    return static_cast<const cudf::numeric_scalar<T>&>(scalar).value();
}


#define ASSERT_IS_VALID(ptr, name) if (!ptr->is_valid()) throw std::runtime_error(std::string{ name } + " is not Valid")
#define ASSERT_SCALAR_IS_VALID(ptr) ASSERT_IS_VALID(ptr, "Scalar")
#define ASSERT_IS_TRUE(check) do { cudf::AssertIsTrue(check, #check); }while (0)


#ifndef CUDA_RT_CALL
#define CUDA_RT_CALL(call)                                                    \
  {                                                                           \
    cudaError_t cudaStatus = call;                                            \
    if (cudaSuccess != cudaStatus)                                            \
      fprintf(stderr,                                                         \
              "ERROR: CUDA RT call \"%s\" in line %d of file %s failed with " \
              "%s (%d).\n",                                                   \
              #call,                                                          \
              __LINE__,                                                       \
              __FILE__,                                                       \
              cudaGetErrorString(cudaStatus),                                 \
              cudaStatus);                                                    \
  }
#endif // CUDA_RT_CALL

template<typename T>
void CopyToGPUArray(
    size_t rowCount,
    pd::ArrayPtr const& column,
    const rmm::cuda_stream_view& stream,
    int64_t columnIndex,
    rmm::device_uvector<T>& arr)
{
    const size_t sizeInBytes = sizeof(T) * rowCount;

    ASSERT_IS_TRUE(column->type()->id() ==
                   typename arrow::CTypeTraits<T>::ArrowType{}.id());
    ASSERT_IS_TRUE(column->null_count() == 0); // TRYING TO BE SAFE FOR NOW

    CUDA_RT_CALL(cudaMemcpyAsync(
        static_cast<void*>(arr.data() + columnIndex * rowCount),
        static_cast<const void*>(column->data()->GetMutableValues<T>(1)),
        sizeInBytes,
        cudaMemcpyKind::cudaMemcpyHostToDevice,
        stream));
}

template<typename T>
void CopyToCPUArray(
    size_t rowCount,
    const rmm::device_uvector<T>& arr,
    const rmm::cuda_stream_view& stream,
    int64_t columnIndex,
    typename arrow::CTypeTraits<T>::ArrayType& array)
{
    const size_t sizeInBytes = sizeof(T) * rowCount;

    CUDA_RT_CALL(cudaMemcpyAsync(
        const_cast<double*>(array.raw_values()),
        static_cast<const void*>(arr.data() + columnIndex * rowCount),
        sizeInBytes,
        cudaMemcpyKind::cudaMemcpyDeviceToHost,
        stream));
}

template<typename T>
rmm::device_uvector<T> ToGPUArray(
    size_t rowCount,
    std::vector<pd::ArrayPtr> const& columns,
    const rmm::cuda_stream_view& stream)
{
    const size_t columnSize = columns.size();

    // Always Skip Index
    rmm::device_uvector<T> arr{ columns.size() * rowCount, stream };
    if (columnSize == 1)
    {
        CopyToGPUArray(rowCount, columns.at(0), stream, 0, arr);
    }
    else
    {
        for (auto const& columnIndex : std::ranges::views::iota(0UL, columnSize))
        {
            CopyToGPUArray(rowCount, columns.at(columnIndex), stream, columnIndex, arr);
        }
    }

    return arr;
}

template<typename T>
rmm::device_uvector<T> ToGPUArray(pd::DataFrame const& df, const rmm::cuda_stream_view& stream)
{
    return ToGPUArray<T>(df.num_rows(), df.array()->columns(), stream);
}

template<typename T>
rmm::device_uvector<T> ToGPUArray(pd::ArrayPtr const& array, const rmm::cuda_stream_view& stream)
{
    const size_t N = static_cast<size_t>(array->length());
    rmm::device_uvector<T> arr{ N, stream };
    CopyToGPUArray<T>(N, array, stream, 0, arr);
    return arr;
}

template<typename T>
pd::ArrayPtr ToCPUArray(rmm::device_uvector<T> const& array, const rmm::cuda_stream_view& stream)
{
    const size_t N = array.size();
    using ArrowType = typename arrow::CTypeTraits<T>::ArrowType;
    using ArrayType = typename arrow::CTypeTraits<T>::ArrayType;
    using ArrayBuilder = typename arrow::CTypeTraits<T>::BuilderType ;

    ArrayBuilder builder;
    pd::ThrowOnFailure(builder.AppendEmptyValues(N));

    auto result = pd::ReturnOrThrowOnFailure(builder.Finish());
    auto& cpuArray = arrow::checked_cast<ArrayType&>(*result);
    CopyToCPUArray<T>(N, array, stream, 0, cpuArray);

    return result;
}
}