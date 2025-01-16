#pragma once
//
// Created by dewe on 12/28/22.
//
#include "arrow/compute/api.h"
#include "core.h"
#include "memory"
#include "concepts"

template<typename T>
concept ScalarComplyable = requires {
    std::is_scalar_v<T> || std::same_as<T, std::string>;
};

#define BINARY_OPERATOR(op, name)    \
DataFrame operator op(DataFrame const& s) const; \
Series operator op(Series const& s) const;  \
inline Scalar operator op(Scalar const& s) const \
    { \
        return Scalar{ReturnOrThrowOnFailure(arrow::compute::CallFunction(#name, { this->scalar, s.scalar })).scalar()}; \
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

#define BINARY_BOOL_OPERATOR(op, name)    \
DataFrame operator op(DataFrame const& s) const; \
Series operator op(Series const& s) const;  \
inline bool operator op(Scalar const& s) const \
    { \
        return ReturnOrThrowOnFailure(arrow::compute::CallFunction(#name, { this->scalar, s.scalar })).scalar_as<arrow::BooleanScalar>().value; \
    } \
    template<typename T> \
    requires std::is_scalar_v<T> \
    inline bool operator op(T const& s) const \
    { \
        return *this op Scalar(s); \
    } \
    template<typename T> requires std::is_scalar_v<T> \
    friend bool operator op(T const& s, Scalar const& rhs) \
    { \
        return Scalar(s) op rhs; \
    }

namespace pd {
    class DataFrame;

    class Series;

    /// The Scalar class is a wrapper for the "arrow::Scalar" class from the
/// Apache Arrow library. It is used for storing scalar values.
/// This class has several constructors that allows user to create Scalar object from different types.
    class Scalar {
    public:
        Scalar() = default;

        Scalar(Scalar const &) = default;

        Scalar(Scalar &&) noexcept = default;

        Scalar &operator=(Scalar const &other) = default;

        Scalar &operator=(Scalar &&other) noexcept = default;

        ~Scalar() = default;

        explicit Scalar(arrow::Result<std::shared_ptr<arrow::Scalar>> const &sc) {
            if (sc.ok()) {
                scalar = sc.ValueUnsafe();
            } else {
                throw std::runtime_error(sc.status().ToString());
            }
        }

        [[nodiscard]] inline bool IsType(arrow::Type::type type) const {
            return scalar->type->id() == type;
        }

        explicit Scalar(std::shared_ptr<arrow::DataType> const &dt) : scalar(arrow::MakeNullScalar(dt)) {}

        explicit Scalar(std::shared_ptr<arrow::Scalar> const &sc) : scalar(sc) {
        }

        explicit Scalar(std::shared_ptr<arrow::Scalar> &&value);

        template<ScalarComplyable T>
        explicit Scalar(T const &x) : scalar(arrow::MakeScalar(x)) {
        }

        explicit Scalar(ptime const &x) : scalar(x.is_not_a_date_time() ? arrow::MakeNullScalar(arrow::timestamp(arrow::TimeUnit::NANO)) : ReturnOrThrowOnFailure(arrow::MakeScalar(arrow::timestamp(arrow::TimeUnit::NANO), pd::fromPTime(x)))) {
        }

        explicit Scalar(date const &x) : Scalar(ptime(x)) {}

        /// template function "as()" that allows the user to cast the Scalar object to a specified type.
        template<class T>
        T as() const {

            if (!scalar->is_valid) {
                if constexpr (std::is_floating_point_v<T>) {
                    return std::numeric_limits<T>::quiet_NaN();
                }
                else if constexpr (std::is_same_v<T, date> || std::is_same_v<T, ptime>) {
                    return T(not_a_date_time);
                }

                throw std::runtime_error("as<>() requires a valid Scalar");
            }

            if constexpr (std::same_as<T, std::string>) {
                return scalar->ToString();
            }
            if constexpr (std::is_same_v<T, date> || std::is_same_v<T, ptime>) {
                if (scalar->type->id() != arrow::Type::TIMESTAMP){
                    throw std::runtime_error("as<date|ptime>() requires a TimestampScalar");
                }
                auto ptimeValue = time_from_string(scalar->ToString());
                if constexpr (std::is_same_v<T, date>) {
                    return ptimeValue.date();
                }
                else {
                    return ptimeValue;
                }
            }
            else {
                using ArrowType = arrow::CTypeTraits<T>::ScalarType;
                auto res = std::dynamic_pointer_cast<ArrowType>(scalar);
                if (res) {
                    if constexpr (std::same_as<T, std::string>) {
                        return res->value->ToString();
                    } else {
                        return res->value;
                    }
                } else {
                    std::stringstream ss;
                    ss << "Invalid Scalar Cast to " << typeid(T).name() << ", current type " << scalar->type->name()
                       << "\n";
                    throw std::runtime_error(ss.str());
                }
            }
        }

        int64_t dt() const {
            auto res = std::dynamic_pointer_cast<arrow::TimestampScalar>(scalar);
            if (res) {
                return res->value;
            }
            std::stringstream ss;
            ss << "Invalid Scalar Cast to TimestampType, current type " << scalar->type->name() << "\n";
            throw std::runtime_error(ss.str());
        }

        [[nodiscard]] inline bool isValid() const {
            return scalar->is_valid;
        }

        [[nodiscard]] inline std::shared_ptr<arrow::Scalar> value() const {
            return scalar;
        }

        template<typename T>
        inline Scalar cast() const {
            return Scalar{ReturnOrThrowOnFailure(value()->CastTo(arrow::CTypeTraits<T>::type_singleton()))};
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

        BINARY_BOOL_OPERATOR(>, greater)

        BINARY_BOOL_OPERATOR(>=, greater_equal)

        BINARY_BOOL_OPERATOR(<, less)

        BINARY_BOOL_OPERATOR(<=, less_equal)

        inline bool operator==(Scalar const &s) const {
            return ReturnOrThrowOnFailure(arrow::compute::CallFunction("equal", {this->scalar, s.scalar}))
                    .scalar_as<arrow::BooleanScalar>()
                    .value;
        }

        template<ScalarComplyable T>
        inline bool operator==(T const &s) const {
            return Scalar{s} == *this;
        }

        template<ScalarComplyable T>
        friend bool operator==(T const &s, Scalar const &rhs) {
            return pd::Scalar{s} == rhs;
        }

        friend std::ostream& operator<<(std::ostream & os, Scalar const& s)
        {
            os << s.scalar->ToString();
            return os;
        }

        DataFrame operator==(DataFrame const &s) const;

        Series operator==(Series const &s) const;

        BINARY_BOOL_OPERATOR(!=, not_equal)

        BINARY_BOOL_OPERATOR(&&, and)

        BINARY_BOOL_OPERATOR(||, or)

        inline bool operator!() const {
            return !as<bool>();
        }

#undef BINARY_OPERATOR

    public:
        std::shared_ptr<arrow::Scalar> scalar{arrow::MakeNullScalar(arrow::null())};
    };

/// Nested struct called "MinMax", which contains two Scalar objects,
/// one for the minimum and one for the maximum value.
    struct MinMax {
        Scalar min, max;
    };

    struct ModeWithCount {
        Scalar mode, count;
    };

    inline Scalar ReturnScalarOrThrowOnError(auto &&result) {
        if (result.ok()) {
            return Scalar{result->scalar()};
        }
        throw std::runtime_error(result.status().ToString());
    }
} // namespace pd