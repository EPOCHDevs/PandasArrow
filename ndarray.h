#pragma once
//
// Created by dewe on 12/29/22.
//
#include <arrow/compute/api_scalar.h>
#include <arrow/api.h>
#include <arrow/scalar.h>
#include "scalar.h"
#include "sstream"
#include <cmath>
#include <iostream>


namespace pd {

struct HashScalar{

    bool operator()(std::shared_ptr<arrow::Scalar> const& a,
                    std::shared_ptr<arrow::Scalar> const& b) const
    {
        return a->Equals(b);
    }

    size_t operator()(std::shared_ptr<arrow::Scalar> const& scalar) const
    {
        return scalar->hash();
    }
};

using Indexer = std::unordered_map<std::shared_ptr<arrow::Scalar>,
                                   int64_t,
                                   HashScalar,
                                   HashScalar>;

/// Thrown when an invalid cast is attempted on the array data.
struct RawArrayCastException : std::exception
{
    std::shared_ptr<arrow::DataType> requested_type, array_type;
    mutable std::string msg;

    RawArrayCastException(
        std::shared_ptr<arrow::DataType> requested_type,
        std::shared_ptr<arrow::DataType> array_type)
        : requested_type(std::move(requested_type)),
          array_type(std::move(array_type))
    {
    }

    const char* what() const noexcept override
    {
        std::stringstream ss;
        ss << "Calling values with wrong data type:\t"
           << "Requested DataType: " << requested_type->ToString() << "\n"
           << "Current DataType: " << array_type->ToString() << "\n";
        msg = ss.str();
        return msg.c_str();
    }
};



template<class BaseT>
struct NDArray
{

public:
    using ArrayType = std::shared_ptr<std::conditional_t<
        std::same_as<BaseT, class DataFrame>,
        arrow::RecordBatch,
        arrow::Array>>;

    ArrayType m_array;

    inline auto array() const
    {
        return m_array;
    }

    NDArray(
        ArrayType const& array,
        std::shared_ptr<arrow::Array> const& _index,
        bool skipIndex = false);

    NDArray(ArrayType const& array, int64_t num_rows)
        : m_array(array), m_index(uint_range(num_rows))
    {
    }

    NDArray(std::shared_ptr<arrow::Array> const& _index=nullptr)
        : m_array(nullptr), m_index(_index)
    {
    }

    NDArray(int64_t num_rows)
        : m_array(nullptr), m_index(uint_range(num_rows))
    {
    }

    NDArray(int64_t num_rows, std::shared_ptr<arrow::Array> const& _index);

    template<typename T>
    static std::vector<bool> makeValidFlags(std::vector<T> const& arr);

    static std::vector<uint8_t> makeValidFlags(
        std::vector<std::string> const& arr);

    virtual bool equals_(BaseT const& a) const
    {
        return m_array->Equals(*a.m_array);
    }

    virtual bool approx_equals_(BaseT const& a) const
    {
        return m_array->ApproxEquals(*a.m_array);
    }

    std::shared_ptr<arrow::Array> indexArray() const noexcept
    {
        return m_index;
    }

protected:
    std::vector<uint8_t> byteValidStr;
    std::shared_ptr<arrow::Array> m_index;
    Indexer indexer;
    bool isIndex{ false };

    std::shared_ptr<arrow::Array> uint_range(int64_t n_rows);

    void setIndexer();

    static inline void throwOnNotOkStatus(arrow::Status const& status)
    {
        if(not status.ok())
            throw std::runtime_error(status.ToString());
    }

    template<class T>
    static T ValidateAndReturn(arrow::Result<T>&& result);
};

template<class BaseT>
NDArray<BaseT>::NDArray(
    ArrayType const& array,
    std::shared_ptr<arrow::Array> const& _index,
    bool skipIndex)
    : m_array(array), m_index(_index)
{
    if (not m_index and not skipIndex)
    {
        if (m_array)
        {
            if constexpr (std::same_as<
                              ArrayType,
                              std::shared_ptr<arrow::RecordBatch>>)
            {
                m_index = uint_range(array->num_rows());
            }
            else
            {
                m_index = uint_range(array->length());
            }
        }
    }
}

template<class BaseT>
NDArray<BaseT>::NDArray(
    int64_t num_rows,
    std::shared_ptr<arrow::Array> const& _index)
    : m_array(nullptr)
{
    if (_index)
    {
        m_index = _index;
    }
    else
    {
        m_index = uint_range(num_rows);
    }
}

template<class BaseT>
std::vector<uint8_t> NDArray<BaseT>::makeValidFlags(
    std::vector<std::string> const& arr)
{

    std::vector<uint8_t> valid;
    valid.reserve(arr.size());

    for (auto const& x : arr)
    {
        valid.push_back(isValid(x));
    }

    return valid;
}

template<class BaseT>
std::shared_ptr<arrow::Array> NDArray<BaseT>::uint_range(int64_t n_rows)
{
    std::vector<uint64_t> index(n_rows);
    std::iota(index.begin(), index.end(), 0);
    arrow::UInt64Builder builder;

    auto status = builder.AppendValues(index);
    if (status.ok())
    {
        return ValidateAndReturn(builder.Finish());
    }

    throwOnNotOkStatus(status);
    return {};
}

template<class BaseT>
void NDArray<BaseT>::setIndexer()
{
    isIndex = true;
    for (int i = 0; i < m_array->length(); i++)
    {
        indexer[m_array->GetScalar(i).MoveValueUnsafe()] = i;
    }
}

template<class BaseT>
template<typename T>
 std::vector<bool> NDArray<BaseT>::makeValidFlags(std::vector<T> const& arr)
{

    std::vector<bool> valid;
    valid.reserve(arr.size());

    for (auto const& x : arr)
    {
        valid.push_back(isValid(x));
    }

    return valid;
}

template<class BaseT>
template<class T>
 T NDArray<BaseT>::ValidateAndReturn(arrow::Result<T>&& result)
{
    if (result.ok())
    {
        return result.MoveValueUnsafe();
    }

    throw std::runtime_error(result.status().ToString());
}

template<class T>
static T ValidateAndReturn(arrow::Result<T>&& result)
{
    if (result.ok())
    {
        return result.MoveValueUnsafe();
    }

    throw std::runtime_error(result.status().ToString());
}

}