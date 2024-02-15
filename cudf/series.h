#pragma once
//
// Created by dewe on 2/7/24.
//
#include "cudf_utils.h"
#include "ranges"

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

        bool any() const
        {
            return cudf::any(data().view());
        }

        bool all() const
        {
            return cudf::all(data().view());
        }

        double mean() const
        {
            return cudf::mean(data().view());
        }

        double std() const
        {
            return cudf::stddev(data().view());
        }

        double var() const
        {
            return cudf::var(data().view());
        }

        std::unique_ptr<cudf::scalar> min() const
        {
            return cudf::min(data().view());
        }

        std::unique_ptr<cudf::scalar> max() const
        {
            return cudf::max(data().view());
        }

        template<typename T>
        rmm::device_uvector<T> ToArray(const rmm::cuda_stream_view& stream)
        {
            const size_t rowCount = m_data->num_rows();
            const size_t sizeInBytes = sizeof(T) * rowCount;

            // Always Skip Index
            rmm::device_uvector<T> arr{ m_data->num_columns() * rowCount, stream };
            for (auto const& columnIndex : std::ranges::views::iota(1, m_data->num_columns()))
            {
                const auto column = m_data->get_column(columnIndex);
                ASSERT_IS_TRUE(column.type() == cudf::type_to_id<T>());
                ASSERT_IS_TRUE(column.null_count() == 0); // TRYING TO BE SAFE FOR NOW

                CUDA_RT_CALL(cudaMemcpyAsync(
                    static_cast<void*>(arr.data() + columnIndex * rowCount),
                    static_cast<const void*>(m_data->get_column(columnIndex).view().data<T>()),
                    sizeInBytes,
                    cudaMemcpyKind::cudaMemcpyDeviceToDevice,
                    stream));
            }
            return arr;
        }

    protected:
        std::vector<cudf::column_metadata> m_columnMetaData;
        std::unique_ptr<cudf::table> m_data;

        GPUSeries() = default;

    };
}  // namespace pd