#pragma once
//
// Created by dewe on 1/2/23.
//

#include <arrow/compute/exec/test_util.h>
#include "unordered_map"
#include "string"
#include "arrow/compute/exec/exec_plan.h"
#include "arrow/compute/api_aggregate.h"
#include "arrow/compute/api_vector.h"
#include "arrow/compute/row/grouper.h"
#include "series.h"


namespace pd {

    class GroupBy {

    public:

        GroupBy( const std::shared_ptr<arrow::Array>& key,
                class std::shared_ptr<arrow::RecordBatch> const &df) : rb(df), key(key) {}

        struct Compute {
            const std::vector<std::shared_ptr<arrow::Field>> columns;
            std::vector<arrow::Datum> column_data;
            Series key_data;

            [[nodiscard]] class DataFrame agg(std::vector<std::string> const &funcs,
                                              bool skip_null = true);

            [[nodiscard]] static DataFrame
            agg(std::vector<arrow::compute::Aggregate> const &funcs,
                Series const &key,
                std::vector<arrow::Datum> const &columns,
                arrow::FieldVector const&,
                bool skip_null = true);

        };

        Compute operator[](std::vector<std::string> const &columns_str);

        const std::shared_ptr<arrow::RecordBatch> &rb;
    private:
        std::shared_ptr<arrow::Array> key;
    };
}