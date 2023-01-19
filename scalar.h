#pragma once
//
// Created by dewe on 12/28/22.
//
#include "core.h"
#include "memory"
#include "arrow/compute/api.h"

namespace pd{
/// The Scalar class is a wrapper for the "arrow::Scalar" class from the
/// Apache Arrow library. It is used for storing scalar values.
/// This class has several constructors that allows user to create Scalar object from different types.
    class Scalar
    {
    public:
        Scalar()=default;

        Scalar(arrow::Result<std::shared_ptr<arrow::Scalar>> const& sc):scalar(nullptr)
        {
            if(sc.ok())
            {
                scalar = sc.ValueUnsafe();
            }
            else
            {
                throw std::runtime_error(sc.status().ToString());
            }
        }

        Scalar(std::shared_ptr<arrow::Scalar> const& sc):scalar(sc)
        {}

        Scalar(std::shared_ptr<arrow::Scalar> && value);

        Scalar(pd::Scalar const& sc) = default;

        Scalar(const char* x):scalar(arrow::MakeScalar(x)) {}

        template<typename T> requires (std::is_scalar_v<T> or std::same_as<T, std::string>)
        Scalar(T const& x):scalar(arrow::MakeScalar(x)) {}

        /// template function "as()" that allows the user to cast the Scalar object to a specified type.
        template<class T>
        T as()
        {
            using ArrowType = arrow::CTypeTraits<T>::ScalarType;
            auto res  = std::dynamic_pointer_cast<ArrowType>(scalar);
            if(res)
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
            else{
                throw std::runtime_error("Invalid Scalar Cast");
            }
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

        inline const std::shared_ptr<arrow::Scalar> value() const
        {
            return scalar;
        }

        template<typename B> requires std::is_scalar_v<B>
        bool operator==(B b) const
        {
            if(std::is_floating_point_v<B>)
            {
                return scalar->ApproxEquals(*arrow::MakeScalar(b));
            }
            return scalar->Equals(arrow::MakeScalar(b));
        }

        template<typename B> requires std::is_scalar_v<B>
        friend bool operator==(std::shared_ptr<arrow::Scalar> const& a, B b)
        {
            if(std::is_floating_point_v<B>)
            {
                return a->ApproxEquals(*arrow::MakeScalar(b));
            }
            return a->Equals(arrow::MakeScalar(b));
        }

        bool operator==(pd::Scalar const& other) const
        {
            return scalar->Equals(other.scalar);
        }

    public:
        std::shared_ptr<arrow::Scalar> scalar{
            arrow::MakeNullScalar(
                    std::make_shared<arrow::DoubleType>())};
    };

    /// Nested struct called "MinMax", which contains two Scalar objects,
    /// one for the minimum and one for the maximum value.
    struct MinMax
    {
        Scalar min, max;
    };
}