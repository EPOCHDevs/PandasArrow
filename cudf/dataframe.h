#pragma once
//
// Created by dewe on 2/7/24.
//
#include "series.h"


namespace pd {

    class GPUDataframe : public GPUSeries {
    public:
        GPUDataframe(std::unique_ptr<cudf::table> newTable, std::vector<cudf::column_metadata> metaData)
                : GPUSeries(std::move(newTable), std::move(metaData)) {}

        GPUDataframe(std::shared_ptr<arrow::Array> const &index,
                     std::shared_ptr<arrow::RecordBatch> const &recordBatch);

        GPUDataframe(class DataFrame const &);

        DataFrame GetDataFrame();

        GPUDataframe operator+(auto const &other) const {
            return binary_operator(other, cudf::binary_operator::ADD);
        }

        GPUDataframe operator*(auto const &other) const {
            return binary_operator(other, cudf::binary_operator::MUL);
        }

    private:
        using GPUSeries::series;

        GPUDataframe binary_operator(GPUDataframe const &other,
                                     cudf::binary_operator op) const;
//        GPUDataframe binary_operator(GPUSeries const &other) const;
//        GPUDataframe binary_operator(GPUDataframe const &other) const;

    };
}  // namespace pd