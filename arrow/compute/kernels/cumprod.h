#pragma once
//
// Created by dewe on 12/29/22.
//

#include "arrow/compute/registry.h"

namespace arrow::compute {

    /// \brief Options for cumulative product function
    class ARROW_EXPORT CumulativeProductOptions : public FunctionOptions {
    public:
        explicit CumulativeProductOptions(double start = 1, bool skip_nulls = false,
                                          bool check_overflow = false);

        explicit CumulativeProductOptions(std::shared_ptr<Scalar> start, bool skip_nulls = false,
                                          bool check_overflow = false);

        static constexpr char const kTypeName[] = "CumulativeProductOptions";

        static CumulativeProductOptions Defaults() { return CumulativeProductOptions(); }

        /// Optional starting value for cumulative operation computation
        std::shared_ptr<Scalar> start;

        /// If true, nulls in the input are ignored and produce a corresponding null output.
        /// When false, the first null encountered is propagated through the remaining output.
        bool skip_nulls = false;

        /// When true, returns an Invalid Status when overflow is detected
        bool check_overflow = false;
    };

    ARROW_EXPORT
    Result<Datum> CumulativeProduct(
            const Datum &values,
            const CumulativeProductOptions &options = CumulativeProductOptions::Defaults(),
            ExecContext *ctx = NULLPTR);

    namespace internal {
        void RegisterVectorCumulativeProduct(FunctionRegistry *registry);
    }
}