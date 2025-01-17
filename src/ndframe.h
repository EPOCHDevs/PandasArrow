#pragma once
//
// Created by dewe on 12/29/22.
//
#include <arrow/api.h>
#include <arrow/compute/api_scalar.h>
#include <arrow/scalar.h>
#include <cmath>
#include <iostream>
#include <fmt/format.h>

#include "scalar.h"
#include "sstream"
#include "span"
#include <DataFrame/DataFrame.h>


namespace pd {

    struct HashScalar {

        bool operator()(std::shared_ptr<arrow::Scalar> const &a, std::shared_ptr<arrow::Scalar> const &b) const {
            return a->Equals(*b->CastTo(a->type).MoveValueUnsafe());
        }

        bool equal(std::shared_ptr<arrow::Scalar> const &a, std::shared_ptr<arrow::Scalar> const &b) const {
            return a->Equals(*b->CastTo(a->type).MoveValueUnsafe());
        }

        size_t operator()(std::shared_ptr<arrow::Scalar> const &scalar) const {
            return scalar->hash();
        }

        size_t hash(std::shared_ptr<arrow::Scalar> const &scalar) const {
            return scalar->hash();
        }
    };

    using Indexer = std::unordered_map<std::shared_ptr<arrow::Scalar>, int64_t, HashScalar, HashScalar>;

/// Thrown when an invalid cast is attempted on the array data.
    struct RawArrayCastException : std::exception {
        std::shared_ptr<arrow::DataType> requested_type, array_type;
        mutable std::string msg;

        RawArrayCastException(std::shared_ptr<arrow::DataType> requested_type,
                              std::shared_ptr<arrow::DataType> array_type)
                : requested_type(std::move(requested_type)), array_type(std::move(array_type)) {
        }

        const char *what() const noexcept override {
            std::stringstream ss;
            ss << "Calling values with wrong data type:\t"
               << "Requested DataType: " << requested_type->ToString() << "\n"
               << "Current DataType: " << array_type->ToString() << "\n";
            msg = ss.str();
            return msg.c_str();
        }
    };

    std::shared_ptr<arrow::Array> uint_range(int64_t n_rows);

    struct Mode {
        Scalar mode;
        int64_t count;
    };

    template<bool expand, typename ReturnT, typename T, typename BaseT, class ArrayType, typename Fn>
    T rollingT(Fn const &fn,
               int64_t window,
               ArrayType const &array,
               std::shared_ptr<arrow::Array> const &index) {
        auto size = index->length();
        typename arrow::CTypeTraits<ReturnT>::BuilderType builder;
        ThrowOnFailure(builder.Reserve(size));

        if (window > size) {
            return {pd::ReturnOrThrowOnFailure(
                    arrow::MakeEmptyArray(arrow::CTypeTraits<ReturnT>::type_singleton())),
                    nullptr};
        }

        for (int64_t i: std::views::iota(0, size - window + 1)) {
            ArrayType subArr;
            pd::ArrayPtr subIndex;

            if constexpr (expand) {
                subArr = array->Slice(0, i + window);
                subIndex = index->Slice(0, i + window);
            } else {
                subArr = array->Slice(i, window);
                subIndex = index->Slice(i, window);
            }
            builder.UnsafeAppend(fn(BaseT(subArr, subIndex)));
        }

        return {ReturnOrThrowOnFailure(builder.Finish()), index->Slice(window - 1)};
    }

    template<class ArrayTypeImpl>
    class NDFrame {

    public:
        using ArrayType = std::shared_ptr<ArrayTypeImpl>;
        using BaseT = NDFrame<ArrayTypeImpl>;
        using ChildType = std::conditional_t<std::same_as<ArrayTypeImpl, arrow::Array>, class Series, class DataFrame>;

        virtual ~NDFrame() = default;

        //<editor-fold desc="Constructor">
        NDFrame(ArrayType const &array, std::shared_ptr<arrow::Array> const &_index, bool skipIndex = false);

        NDFrame(ArrayType const &array, int64_t num_rows);

        explicit NDFrame(std::shared_ptr<arrow::Array> const &_index = nullptr);

        explicit NDFrame(int64_t num_rows);

        NDFrame(int64_t num_rows, std::shared_ptr<arrow::Array> const &_index);

        virtual bool equals_(BaseT const &a) const {
            return m_array->Equals(*a.m_array);
        }

        virtual bool approx_equals_(BaseT const &a) const {
            return m_array->ApproxEquals(*a.m_array);
        }

        [[nodiscard]] Series index() const;

        std::shared_ptr<arrow::Array> indexArray() const noexcept {
            return m_index;
        }

        template<typename T>
        std::span<const T> getIndexSpan() const {
            static_assert(arrow::is_fixed_width_type<typename arrow::CTypeTraits<T>::ArrowType>::value, "Type must be fixed width");
            if (arrow::CTypeTraits<T>::type_singleton()->id() != m_index->type()->id())
            {
                throw std::runtime_error(fmt::format("Type mismatch: expected {}, got {}", m_index->type()->ToString(), arrow::CTypeTraits<T>::type_singleton()->ToString()));
            }
            return {reinterpret_cast<const T *>(m_index->data()->buffers[1]->data()) + m_index->offset(),
                    size_t(m_index->length())};
        }

        template<typename T>
        std::shared_ptr<typename arrow::CTypeTraits<T>::ArrayType> getIndexView() const {
            static_assert(arrow::is_fixed_width_type<typename arrow::CTypeTraits<T>::ArrowType>::value, "Type must be fixed width");
            if (arrow::CTypeTraits<T>::type_singleton()->id() != m_index->type()->id())
            {
                throw std::runtime_error(fmt::format("Type mismatch: expected {}, got {}", m_index->type()->ToString(), arrow::CTypeTraits<T>::type_singleton()->ToString()));
            }
            return std::static_pointer_cast<typename arrow::CTypeTraits<T>::ArrayType>(m_index);
        }
        //</editor-fold>

        //<editor-fold desc="Aggregation Functions">
        [[nodiscard]] bool all(bool skip_null = true) const;

        [[nodiscard]] bool any(bool skip_null = true) const;

        [[nodiscard]] Scalar median(bool skip_null = true) const;

        [[nodiscard]] int64_t count() const;

        [[nodiscard]] int64_t count_na() const;

        [[nodiscard]] int64_t nunique() const;

        [[nodiscard]] Scalar first(bool skip_null = true) const;

        [[nodiscard]] std::array<Scalar, 2> first_last(bool skip_null = true) const;

        [[nodiscard]] int64_t index(Scalar const &search) const;

        [[nodiscard]] Scalar last(bool skip_null = true) const;

        [[nodiscard]] Scalar max(bool skip_null = true) const;

        [[nodiscard]] Scalar mean(bool skip_null = true) const;

        [[nodiscard]] Scalar min(bool skip_null = true) const;

        [[nodiscard]] MinMax min_max(bool skip_nulls) const;

        [[nodiscard]] std::vector<Mode> mode(int64_t n, bool skip_nulls, uint32_t minCount = 1) const;

        [[nodiscard]] Scalar product(bool skip_null = true) const;

        [[nodiscard]] Scalar quantile(double q = 0.5,
                                      arrow::compute::QuantileOptions::Interpolation interpolation =
                                      arrow::compute::QuantileOptions::Interpolation::LINEAR,
                                      bool skip_nulls = true, uint32_t min_count = 0) const;

        [[nodiscard]] Scalar std(int ddof = 1, bool skip_na = true) const;

        [[nodiscard]] Scalar sum(bool skip_null = true) const;

        [[nodiscard]] Scalar tdigest(double q = 0.5, uint32_t delta = 100,
                                     uint32_t buffer_size = 500, bool skip_nulls = true,
                                     uint32_t min_count = 0) const;

        [[nodiscard]] Scalar var(int ddof = 1, bool skip_na = true) const;

        [[nodiscard]] Scalar agg(std::string const &func, bool skip_null = true) const;

        [[nodiscard]] int64_t argmax() const {
            return index(max());
        }

        [[nodiscard]] int64_t argmin() const {
            return index(min());
        }
        //</editor-fold>

        //<editor-fold desc="Indexing Functions">
        virtual ChildType operator[](Slice) const = 0;
        ChildType operator[](Series const &) const;
        ChildType operator[](DateTimeSlice const &) const;
        ChildType operator[](DateSlice const &s) const;
        ChildType operator[](StringSlice const &) const;

        ChildType loc(DateTimeSlice const &) const;
        ChildType loc(DateSlice const &s) const;
        ChildType loc(StringSlice const &) const;

        virtual ChildType where(Series const &) const = 0;

        virtual ChildType take(Series const &) const = 0;

        ChildType setIndex(std::shared_ptr<arrow::Array> const &index) const;
        //</editor-fold>

        //<editor-fold desc="Indexing Operations">
        pd::ArrayPtr normalizeIndex() const;
        //</editor-fold>

        ArrayType m_array;
        inline auto array() const {
            return m_array;
        }

    protected:
        std::vector<uint8_t> byteValidStr;
        std::shared_ptr<arrow::Array> m_index;
        Indexer indexer;
        bool isIndex{false};

        void setIndexer() {
            if constexpr (std::same_as<ArrayTypeImpl, arrow::Array>) {
                if (m_array != nullptr) {
                    isIndex = true;
                    for (int i = 0; i < m_array->length(); i++) {
                        indexer[m_array->GetScalar(i).MoveValueUnsafe()] = i;
                    }
                }
            }
        }

        arrow::Datum GetInternalArray() const {
            if constexpr (std::same_as<ArrayTypeImpl, arrow::Array>) {
                return m_array;
            } else {
                return std::make_shared<arrow::ChunkedArray>(m_array->columns());
            }
        }
    };

    extern template
    class NDFrame<arrow::Array>;

    extern template
    class NDFrame<arrow::RecordBatch>;
} // namespace pd