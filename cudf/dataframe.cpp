//
// Created by dewe on 2/7/24.
//

#include "dataframe.h"
#include <cudf/aggregation.hpp>
#include <cudf/filling.hpp>
#include "../dataframe.h"


namespace pd
{

GPUDataframe::GPUDataframe(
    std::shared_ptr<arrow::Array> const& index,
    std::shared_ptr<arrow::RecordBatch> const& recordBatch)
    : GPUSeries()
{
    std::vector<std::shared_ptr<arrow::Array>> arrays{ index };
    std::vector<std::shared_ptr<arrow::Field>> fields{ arrow::field(RESERVED_INDEX_NAME, index->type()) };

    auto const& currentColumns = recordBatch->columns();
    auto const& currentFields = recordBatch->schema()->fields();

    arrays.insert(arrays.end(), currentColumns.begin(), currentColumns.end());
    fields.insert(fields.end(), currentFields.begin(), currentFields.end());

    m_columnMetaData.resize(currentFields.size());
    std::ranges::transform(
        currentFields,
        m_columnMetaData.begin(),
        [](auto&& field) { return cudf::column_metadata{ field->name() }; });

    auto table = arrow::Table::Make(arrow::schema(fields), arrays);
    m_data = cudf::from_arrow(*table, rmm::cuda_stream_per_thread);
}

GPUDataframe::GPUDataframe(DataFrame const& df) : GPUDataframe(df.indexArray(), df.array())
{}

DataFrame GPUDataframe::GetDataFrame()
{
    auto arrowRecordBatch = pd::ReturnOrThrowOnFailure(
        cudf::to_arrow(m_data->view(), m_columnMetaData, rmm::cuda_stream_per_thread)->CombineChunksToBatch());

    auto index = arrowRecordBatch->column(0);
    return { pd::ReturnOrThrowOnFailure(arrowRecordBatch->RemoveColumn(0)), index };
}

GPUDataframe GPUDataframe::binary_operator(GPUDataframe const& other, cudf::binary_operator op) const
{
    if (other.m_columnMetaData != m_columnMetaData)
    {
        throw std::runtime_error("Cannot Operate on different cuda dataframes");
    }

    auto isIndexNotEqualColumn = cudf::any(cudf::binary_operation(
                                               m_data->get_column(0),
                                               other.m_data->get_column(0),
                                               cudf::binary_operator::NOT_EQUAL,
                                               cudf::data_type(cudf::type_id::BOOL8))
                                               ->view());
    if (isIndexNotEqualColumn)
    {
        throw std::runtime_error("Cannot Operate on cuda dataframes with different indexes");
    }

    const size_t columnLength = m_columnMetaData.size();

    std::vector<std::unique_ptr<cudf::column>> columns(columnLength);
    columns[0] = std::make_unique<cudf::column>(m_data->get_column(0));

    std::ranges::transform(
        std::views::iota(1UL, columnLength),
        columns.begin() + 1,
        [&](size_t columnIndex) -> std::unique_ptr<cudf::column>
        { return cudf::binary_operation(m_data->get_column(columnIndex), other.m_data->get_column(columnIndex), op); });

    return GPUDataframe{ std::make_unique<cudf::table>(std::move(columns)), m_columnMetaData };
}

}  // namespace pd