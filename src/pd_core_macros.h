#pragma once
#include "tbb/parallel_for.h"


#define GROUPBY_NUMERIC_AGG(func, T) \
    arrow::Result<pd::DataFrame> GroupBy::func(std::vector<std::string> const& args) \
    { \
        auto schema = df.m_array->schema(); \
        auto N = groups.size(); \
\
        auto fv = fieldVectors(args, schema); \
        arrow::ArrayDataVector arr(args.size()); \
\
        for (size_t i = 0; i < args.size(); ++i) \
        { \
            const auto& arg = args[i]; \
            const auto L = static_cast<size_t>(uniqueKeys->length()); \
            int index = schema->GetFieldIndex(arg); \
\
            std::vector<T> result(L); \
            tbb::parallel_for( \
                tbb::blocked_range<size_t>(0, uniqueKeys->length()), \
                [&](const tbb::blocked_range<size_t>& /*unused*/) \
                { \
                    for (size_t j = 0; j < L; j++) \
                    { \
                        auto key = uniqueKeys->GetScalar(long(j)).MoveValueUnsafe(); \
                        auto& group = groups.at(key); \
\
                        arrow::Datum d = \
                            arrow::compute::CallFunction(#func, { group[index] }, defaultOpt.get()).MoveValueUnsafe(); \
                        result[j] = d.scalar_as<arrow::CTypeTraits<T>::ScalarType>().value; \
                    } \
                }); \
\
            auto builder = arrow::CTypeTraits<T>::BuilderType(); \
            ThrowOnFailure(builder.AppendValues(result)); \
\
            std::shared_ptr<arrow::ArrayData> data; \
            ThrowOnFailure(builder.FinishInternal(&data)); \
            arr[i] = data; \
        } \
\
        return pd::DataFrame(arrow::schema(fv), long(N), arr, uniqueKeys); \
    } \
\
    arrow::Result<pd::Series> GroupBy::func(std::string const& arg) \
    { \
        auto schema = df.m_array->schema(); \
        auto fv = schema->GetFieldByName(arg); \
\
        long L = uniqueKeys->length(); \
        int index = schema->GetFieldIndex(arg); \
\
        std::vector<T> result(L); \
        tbb::parallel_for( \
            tbb::blocked_range<size_t>(0, L), \
            [&](const tbb::blocked_range<size_t>& r) \
            { \
                for (size_t j = r.begin(); j != r.end(); ++j) \
                { \
                    auto key = uniqueKeys->GetScalar(long(j)).MoveValueUnsafe(); \
                    auto& group = groups.at(key); \
\
                    arrow::Datum d = \
                        arrow::compute::CallFunction(#func, { group[index] }, defaultOpt.get()).MoveValueUnsafe(); \
                    result[j] = d.scalar_as<arrow::CTypeTraits<T>::ScalarType>().value; \
                } \
            }); \
\
        auto builder = arrow::CTypeTraits<T>::BuilderType(); \
        ThrowOnFailure(builder.AppendValues(result)); \
\
        std::shared_ptr<arrow::Array> data; \
        ThrowOnFailure(builder.Finish(&data)); \
\
        return pd::Series(data, uniqueKeys); \
    }

#define GROUPBY_AGG(func) \
    arrow::Result<pd::DataFrame> GroupBy::func(std::vector<std::string> const& args) \
    { \
        auto schema = df.m_array->schema(); \
        auto num_groups = groups.size(); \
        auto fv = fieldVectors(args, schema); \
        arrow::ArrayDataVector arr(args.size()); \
        for (size_t i = 0; i < args.size(); i++) \
        { \
            const auto& arg = args[i]; \
            long L = uniqueKeys->length(); \
            int index = schema->GetFieldIndex(arg); \
\
            arrow::ScalarVector result(L); \
            tbb::parallel_for( \
                tbb::blocked_range<size_t>(0, L), \
                [&](const tbb::blocked_range<size_t>& r) \
                { \
                    for (size_t j = r.begin(); j != r.end(); ++j) \
                    { \
                        auto key = uniqueKeys->GetScalar(long(j)).MoveValueUnsafe(); \
                        auto& group = groups.at(key); \
                        arrow::Datum d = pd::ReturnOrThrowOnFailure( \
                            arrow::compute::CallFunction(#func, { group[index] }, defaultOpt.get())); \
                        result[j] = d.scalar(); \
                    } \
                }); \
\
            ARROW_ASSIGN_OR_RAISE(arr[i], buildData(result)); \
        } \
\
        return pd::DataFrame(arrow::schema(fv), long(num_groups), arr, uniqueKeys); \
    } \
\
    arrow::Result<pd::Series> GroupBy::func(std::string const& arg) \
    { \
        auto schema = df.m_array->schema(); \
        auto fv = schema->GetFieldByName(arg); \
        long L = uniqueKeys->length(); \
        int index = schema->GetFieldIndex(arg); \
\
        arrow::ScalarVector result(L); \
        tbb::parallel_for( \
            tbb::blocked_range<size_t>(0, L), \
            [&](const tbb::blocked_range<size_t>& r) \
            { \
                for (size_t j = r.begin(); j != r.end(); ++j) \
                { \
                    auto key = uniqueKeys->GetScalar(long(j)).MoveValueUnsafe(); \
                    auto& group = groups.at(key); \
\
                    arrow::Datum d = \
                        arrow::compute::CallFunction(#func, { group[index] }, defaultOpt.get()).MoveValueUnsafe(); \
                    result[j] = d.scalar(); \
                } \
            }); \
\
        std::shared_ptr<arrow::ArrayBuilder> builder; \
        if (not result.empty()) \
        { \
            builder = arrow::MakeBuilder(result.back()->type).MoveValueUnsafe(); \
            RETURN_NOT_OK(builder->AppendScalars(result)); \
        } \
        std::shared_ptr<arrow::Array> data; \
        RETURN_NOT_OK(builder->Finish(&data)); \
\
        return pd::Series(data, uniqueKeys); \
    }


#define FOR_ALL_COLUMN(SERIES_FUNCTION_NAME) \
    DataFrame DataFrame::SERIES_FUNCTION_NAME() const \
    { \
        auto N = num_columns(); \
        arrow::ArrayDataVector new_columns(N); \
        tbb::parallel_for( \
            0L, \
            N, \
            [&](::int64_t i) \
            { new_columns[i] = Series(this->m_array->column(i), m_index).SERIES_FUNCTION_NAME().m_array->data(); }); \
        return DataFrame(m_array->schema(), num_rows(), new_columns, m_index); \
    }
