//
// Created by dewe on 2/8/24.
//

#include "series.h"
#include "../dataframe.h"
#include "../series.h"


namespace pd {

    GPUSeries::GPUSeries(std::unique_ptr<cudf::column> index, std::unique_ptr<cudf::column> newColumn,
                         cudf::column_metadata metaData)
            : m_columnMetaData({std::move(metaData)}) {
        std::vector<std::unique_ptr<cudf::column>> columns;
        columns.emplace_back(std::move(index));
        columns.emplace_back(std::move(newColumn));

        m_data = std::make_unique<cudf::table>(std::move(columns));
    }

    GPUSeries::GPUSeries(std::shared_ptr<arrow::Array> const &index,
                         std::shared_ptr<arrow::Array> const &array,
                         const std::string &name) {
        std::vector<std::shared_ptr<arrow::Array>> arrays{index, array};
        std::vector<std::shared_ptr<arrow::Field>> fields{
                arrow::field(RESERVED_INDEX_NAME, index->type()),
                arrow::field(name, array->type())};

        m_columnMetaData.emplace_back(cudf::column_metadata(name));
        auto table = arrow::Table::Make(arrow::schema(fields), arrays);
        m_data = cudf::from_arrow(*table);
    }

    GPUSeries::GPUSeries(Series const &series) : GPUSeries(series.indexArray(), series.array()) {}

    Series GPUSeries::series() {
        auto arrowRecordBatch =
                pd::ReturnOrThrowOnFailure(cudf::to_arrow(m_data->view(), m_columnMetaData)->CombineChunksToBatch());

        auto index = arrowRecordBatch->column(0);
        return {arrowRecordBatch->column(0), arrowRecordBatch->column(1), m_columnMetaData[0].name};
    }

    GPUSeries GPUSeries::operator+(GPUSeries const &other) const {
        if (other.m_columnMetaData != m_columnMetaData) {
            throw std::runtime_error("Cannot Operate on different colum metadata");
        }

//        auto isIndexNotEqualColumn = cudf::binary_operation(
//                index(),
//                other.index(),
//                cudf::binary_operator::NOT_EQUAL,
//                cudf::data_type(cudf::type_id::BOOL8));
//
//
//    if (isIndexNotEqual)
//    {
//        throw std::runtime_error("Cannot Operate on cuda dataframes with different indexes");
//    }

        const size_t columnLength = m_columnMetaData.size();

        std::vector<std::unique_ptr<cudf::column>> columns(columnLength);
        columns[0] = std::make_unique<cudf::column>(m_data->get_column(0));

        std::ranges::transform(
                std::views::iota(1UL, columnLength),
                columns.begin() + 1,
                [&](size_t columnIndex) -> std::unique_ptr<cudf::column> {
                    return cudf::binary_operation(
                            m_data->get_column(columnIndex),
                            other.m_data->get_column(columnIndex),
                            cudf::binary_operator::ADD,
                            cudf::data_type(cudf::type_id::FLOAT64));
                });

        return {std::make_unique<cudf::table>(std::move(columns)), m_columnMetaData};
    }
}