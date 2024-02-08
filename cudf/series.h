#pragma once
//
// Created by dewe on 2/7/24.
//
#include "cudf_utils.h"


namespace pd {

    class GPUSeries {
    public:

        GPUSeries(std::unique_ptr<cudf::table> newTable, std::vector<cudf::column_metadata> metaData)
                : m_data(std::move(newTable)), m_columnMetaData(std::move(metaData)) {}

        GPUSeries(std::unique_ptr<cudf::column> index, std::unique_ptr<cudf::column> newColumn,
                  cudf::column_metadata metaData);

        GPUSeries(std::shared_ptr<arrow::Array> const &index, std::shared_ptr<arrow::Array> const &array,
                  const std::string &name = "");

        GPUSeries(class Series const &);

        Series series();

        GPUSeries operator+(GPUSeries const &other) const;

        const cudf::column &index() const {
            return m_data->get_column(0);
        }

        const cudf::column &data() const {
            return m_data->get_column(1);
        }

        bool any()
        {
            return cudf::any(data().view());
        }

        bool all()
        {
            return cudf::all(data().view());
        }

    protected:
        std::vector<cudf::column_metadata> m_columnMetaData;
        std::unique_ptr<cudf::table> m_data;

        GPUSeries() = default;

    };
}  // namespace pd