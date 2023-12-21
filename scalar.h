#pragma once
//
// Created by dewe on 12/28/22.
//
#include "arrow/compute/api.h"
#include "core.h"
#include "memory"

#define BINARY_OPERATOR(op, name) \
    inline Scalar operator op(Scalar const& s) const \
    { \
        return ReturnOrThrowOnFailure(arrow::compute::CallFunction(#name, { this->scalar, s.scalar })).scalar(); \
    } \
    template<typename T> \
    requires std::is_scalar_v<T> \
    inline Scalar operator op(T const& s) const \
    { \
        return *this op Scalar(s); \
    } \
    template<typename T> \
    requires std::is_scalar_v<T> \
    friend Scalar operator op(T const& s, Scalar const& rhs) \
    { \
        return Scalar(s) op rhs; \
    }

namespace pd {
/// The Scalar class is a wrapper for the "arrow::Scalar" class from the
/// Apache Arrow library. It is used for storing scalar values.
/// This class has several constructors that allows user to create Scalar object from different types.
class Scalar
{
public:
    Scalar() = default;

    Scalar(arrow::Result<std::shared_ptr<arrow::Scalar>> const& sc) : scalar(nullptr)
    {
        if (sc.ok())
        {
            scalar = sc.ValueUnsafe();
        }
        else
        {
            throw std::runtime_error(sc.status().ToString());
        }
    }

    inline bool operator==(Scalar const& s) const
    {
        return ReturnOrThrowOnFailure(arrow::compute::CallFunction("equal", { this->scalar, s.scalar }))
            .scalar_as<arrow::BooleanScalar>()
            .value;
    }

    inline bool IsType(arrow::Type::type type) const
    {
        return scalar->type->id() == type;
    }

    template<typename T>
    requires std::is_scalar_v<T>
    inline bool operator==(T const& s) const
    {
        return *this == Scalar(s);
    }
    template<typename T>
    requires std::is_scalar_v<T>
    friend bool operator==(T const& s, Scalar const& rhs)
    {
        return Scalar(s) == rhs;
    }

    Scalar(std::shared_ptr<arrow::Scalar> const& sc) : scalar(sc)
    {
    }

    Scalar(std::shared_ptr<arrow::Scalar>&& value);

    Scalar(pd::Scalar const& sc) = default;

    Scalar(const char* x) : scalar(arrow::MakeScalar(x))
    {
    }

    template<typename T>
    requires(std::is_scalar_v<T> or std::same_as<T, std::string>) Scalar(T const& x) : scalar(arrow::MakeScalar(x))
    {
    }

    /// template function "as()" that allows the user to cast the Scalar object to a specified type.
    template<class T>
    T as() const
    {
        using ArrowType = arrow::CTypeTraits<T>::ScalarType;
        auto res = std::dynamic_pointer_cast<ArrowType>(scalar);
        if (res)
        {
            if constexpr (std::same_as<T, std::string>)
            {
                return res->value->ToString();
            }
            else
            {
                return res->value;
            }
        }
        else
        {
            std::stringstream ss;
            ss << "Invalid Scalar Cast to " << typeid(T).name() << ", current type " << scalar->type->name() << "\n";
            throw std::runtime_error(ss.str());
        }
    }

    int64_t dt() const
    {
        auto res = std::dynamic_pointer_cast<arrow::TimestampScalar>(scalar);
        if (res)
        {
            return res->value;
        }
        std::stringstream ss;
        ss << "Invalid Scalar Cast to TimestampType, current type " << scalar->type->name() << "\n";
        throw std::runtime_error(ss.str());
    }

    inline bool isValid() const
    {
        return scalar->is_valid;
    }

    friend std::ostream& operator<<(std::ostream& os, Scalar const& df)
    {
        os << df.scalar << "\n";
        return os;
    }

    inline std::shared_ptr<arrow::Scalar> value() const
    {
        return scalar;
    }

    operator bool();

    template<typename T>
    inline Scalar cast() const
    {
        return ReturnOrThrowOnFailure(value()->CastTo(arrow::CTypeTraits<T>::type_singleton()));
    }

    BINARY_OPERATOR(/, divide)
    BINARY_OPERATOR(+, add)
    BINARY_OPERATOR(-, subtract)
    BINARY_OPERATOR(*, multiply)
    BINARY_OPERATOR(|, bit_wise_or)
    BINARY_OPERATOR(&, bit_wise_and)
    BINARY_OPERATOR(^, bit_wise_xor)
    BINARY_OPERATOR(<<, shift_left)
    BINARY_OPERATOR(>>, shift_right)
    BINARY_OPERATOR(>, greater)
    BINARY_OPERATOR(>=, greater_equal)
    BINARY_OPERATOR(<, less)
    BINARY_OPERATOR(<=, less_equal)
    //        BINARY_OPERATOR(==, equal)
    BINARY_OPERATOR(!=, not_equal)
    BINARY_OPERATOR(&&, and)
    BINARY_OPERATOR(||, or)
#undef BINARY_OPERATOR

public:
    std::shared_ptr<arrow::Scalar> scalar{ arrow::MakeNullScalar(std::make_shared<arrow::DoubleType>()) };
};

/// Nested struct called "MinMax", which contains two Scalar objects,
/// one for the minimum and one for the maximum value.
struct MinMax
{
    Scalar min, max;
};

struct ModeWithCount
{
    Scalar mode, count;
};
} // namespace pd