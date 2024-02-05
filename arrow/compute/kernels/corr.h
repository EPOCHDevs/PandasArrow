#pragma once
#include "cov.h"
#include <cmath>

namespace arrow {
namespace compute::internal {

template<typename ArrowType>
struct CorrelationState
{
    double mx2;
    double my2;
    CovarianceState<ArrowType> covarianceState;
    CorrelationState(int32_t decimal_scale, const VarianceOptions& options)
        : covarianceState(decimal_scale, options), mx2(0.0), my2(0.0)
    {
    }

    using CType = typename TypeTraits<ArrowType>::CType;

    void Consume(const ArraySpan& x, const ArraySpan& y)
    {
        covarianceState.Consume(x, y);

        mx2 = internal::SumArray<CType, double, SimdLevel::NONE>(
            x,
            [this](CType value)
            {
                const double v = covarianceState.ToDouble(value);
                return (v - this->covarianceState.mean_x) * (v - this->covarianceState.mean_x);
            });

        my2 = internal::SumArray<CType, double, SimdLevel::NONE>(
            y,
            [this](CType value)
            {
                const double v = covarianceState.ToDouble(value);
                return (v - this->covarianceState.mean_y) * (v - this->covarianceState.mean_y);
            });
    }

    static inline void MergeVarStd2(
        int64_t count1,
        double mean1,
        int64_t count2,
        double mean2,
        double m22,
        double* out_m2)
    {
        double mean = (mean1 * count1 + mean2 * count2) / (count1 + count2);
        *out_m2 += m22 + count1 * (mean1 - mean) * (mean1 - mean) + count2 * (mean2 - mean) * (mean2 - mean);
    }

    void MergeFrom(const CorrelationState& other)
    {
        auto count1 = covarianceState.count;
        auto meanx = covarianceState.mean_x;
        auto mean_y = covarianceState.mean_y;
        covarianceState.MergeFrom(other.covarianceState);
        MergeVarStd2(count1, meanx, other.covarianceState.count, other.covarianceState.mean_x, other.mx2, &mx2);
        MergeVarStd2(count1, mean_y, other.covarianceState.count, other.covarianceState.mean_y, other.my2, &my2);
    }
};

template<typename ArrowType>
struct CorrelationImpl : public ScalarAggregator
{
    using ArrayType = typename TypeTraits<ArrowType>::ArrayType;
    using ThisType = CorrelationImpl<ArrowType>;

    std::shared_ptr<DataType> out_type;
    CorrelationState<ArrowType> state;

    explicit CorrelationImpl(
        int32_t decimal_scale,
        const std::shared_ptr<DataType>& out_type,
        const VarianceOptions& options)
        : out_type(out_type), state(decimal_scale, options)
    {
    }

    Status Consume(KernelContext*, const ExecSpan& batch) override
    {
        if (batch[0].is_array() && batch[1].is_array())
        {
            this->state.Consume(batch[0].array, batch[1].array);
        }
        else
        {
            return Status::Invalid("Input must be arrays");
        }
        return Status::OK();
    }

    Status MergeFrom(KernelContext*, KernelState&& src) override
    {
        const auto& other = checked_cast<const ThisType&>(src);
        this->state.MergeFrom(other.state);
        return Status::OK();
    }

    Status Finalize(KernelContext*, Datum* out) override
    {
        auto cov_state = state.covarianceState;

        if (cov_state.count <= cov_state.options.ddof || cov_state.count < cov_state.options.min_count ||
            (!cov_state.all_valid && !cov_state.options.skip_nulls))
        {
            out->value = std::make_shared<DoubleScalar>();
        }
        else
        {
            double std_x = std::sqrt(state.mx2 / (cov_state.count - cov_state.options.ddof));
            double std_y = std::sqrt(state.my2 / (cov_state.count - cov_state.options.ddof));
            double covar = cov_state.m_xy / double(cov_state.count - cov_state.options.ddof);
            double corr = covar / (std_x * std_y);
            out->value = std::make_shared<DoubleScalar>(corr);
        }
        return Status::OK();
    }
};

struct CorrelationInitState
{
    std::unique_ptr<KernelState> state;
    KernelContext* ctx;
    const DataType& in_type_x;
    const DataType& in_type_y;
    const std::shared_ptr<DataType>& out_type;
    const VarianceOptions& options;

    CorrelationInitState(
        KernelContext* ctx,
        const DataType& in_type_x,
        const DataType& in_type_y,
        const std::shared_ptr<DataType>& out_type,
        const VarianceOptions& options)
        : ctx(ctx), in_type_x(in_type_x), in_type_y(in_type_y), out_type(out_type), options(options)
    {
    }

    Status Visit(const DataType&)
    {
        return Status::NotImplemented("No covariance implemented");
    }

    Status Visit(const HalfFloatType&)
    {
        return Status::NotImplemented("No covariance implemented");
    }

    template<typename Type>
    enable_if_number<Type, Status> Visit(const Type&)
    {
        state.reset(new CorrelationImpl<Type>(/*decimal_scale=*/0, out_type, options));
        return Status::OK();
    }

    template<typename Type>
    enable_if_decimal<Type, Status> Visit(const Type&)
    {
        state.reset(new CorrelationImpl<Type>(checked_cast<const DecimalType&>(in_type_x).scale(), out_type, options));
        return Status::OK();
    }

    Result<std::unique_ptr<KernelState>> Create()
    {
        RETURN_NOT_OK(VisitTypeInline(in_type_x, this));
        RETURN_NOT_OK(VisitTypeInline(in_type_y, this));
        return std::move(state);
    }
};

static Result<std::unique_ptr<KernelState>> CorrelationInit(KernelContext* ctx, const KernelInitArgs& args)
{
    CorrelationInitState visitor(
        ctx,
        *args.inputs[0].type,
        *args.inputs[1].type,
        args.kernel->signature->out_type().type(),
        static_cast<const VarianceOptions&>(*args.options));
    return visitor.Create();
}

static void AddCorrelationKernels(
    KernelInit init,
    const std::vector<std::shared_ptr<DataType>>& types,
    ScalarAggregateFunction* func)
{
    for (const auto& ty : types)
    {
        auto sig = KernelSignature::Make({ InputType(ty->id()), InputType(ty->id()) }, float64());
        AddAggKernel(std::move(sig), init, func);
    }
}

const FunctionDoc correlation_doc{ "Calculate the covariance of 2 numeric array",
                                   ("The covariance function computes the covariance of two arrays, "
                                    "array_x and array_y. The covariance measures the degree to"
                                    " which two random variables are linearly related. "
                                    "A positive covariance indicates that the variables "
                                    "increase together, while a negative covariance "
                                    "indicates that the variables vary in opposite directions."
                                    " A covariance of zero indicates that the variables are"
                                    " independent. The function supports both integer "
                                    "and floating-point arrays and skips null values "
                                    "if specified in the filters. The result is returned as a double."),
                                   { "array1", "array2" },
                                   "VarianceOptions" };

static std::shared_ptr<ScalarAggregateFunction> AddCorrelationKernels()
{
    static auto default_std_options = VarianceOptions::Defaults();
    auto func =
        std::make_shared<ScalarAggregateFunction>("corr", Arity::Binary(), correlation_doc, &default_std_options);
    AddCorrelationKernels(CorrelationInit, NumericTypes(), func.get());
    AddCorrelationKernels(CorrelationInit, { decimal128(1, 1), decimal256(1, 1) }, func.get());
    return func;
}

static void RegisterScalarAggregateCorrelation(FunctionRegistry* registry)
{
    DCHECK_OK(registry->AddFunction(AddCorrelationKernels()));
}

} // namespace compute::internal

static Result<arrow::Datum> Correlation(
    std::shared_ptr<arrow::Array> const& arrayx,
    std::shared_ptr<arrow::Array> const& arrayy,
    compute::VarianceOptions const& options)
{
    return CallFunction("corr", { arrayx, arrayy }, &options);
}
} // namespace arrow