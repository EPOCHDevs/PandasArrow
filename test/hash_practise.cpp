//
// Created by dewe on 1/23/23.
//
#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/compute/api.h>
#include <arrow/compute/api_vector.h>
#include <arrow/compute/cast.h>
#include <arrow/compute/exec/exec_plan.h>
#include <arrow/csv/api.h>
#include <arrow/dataset/dataset.h>
#include <arrow/dataset/file_base.h>
#include <arrow/dataset/file_parquet.h>
#include <arrow/dataset/plan.h>
#include <arrow/dataset/scanner.h>
#include <arrow/io/interfaces.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/api.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/table.h>
#include <arrow/util/future.h>
#include <arrow/util/range.h>
#include <arrow/util/thread_pool.h>
#include <arrow/util/vector.h>
#include <iostream>
#include <memory>
#include <utility>
#include "../pandas_arrow.h"


namespace cp = ::arrow::compute;


pd::DataFrame joinOnIndex(
    pd::DataFrame const& left_table,
    pd::DataFrame const& right_table,
    bool ignore_index,
    bool inner = false)
{
    // Convert DataFrames to tables
    auto unique_index = std::to_string(std::chrono::high_resolution_clock::now().time_since_epoch().count()) + "index";
    auto left_table_as_table = left_table.toTable(unique_index);
    auto right_table_as_table = right_table.toTable(unique_index);

    // Define merge options for columns in case of name conflicts
    arrow::Field::MergeOptions merge_options;
    merge_options.promote_nullability = true; // Rename conflicting columns

    // Concatenate tables
    arrow::ConcatenateTablesOptions options;
    options.unify_schemas = true; // Automatically merge schemas of tables
    options.field_merge_options = merge_options; // Use defined merge options for columns

    auto table =
        pd::ReturnOrThrowOnFailure(arrow::ConcatenateTables({ left_table_as_table, right_table_as_table }, options));

    auto merged_rb = pd::ReturnOrThrowOnFailure(table->CombineChunksToBatch());

    auto index_column_i = merged_rb->schema()->GetFieldIndex(unique_index);
    auto index = merged_rb->column(index_column_i);

    merged_rb = pd::ReturnOrThrowOnFailure(merged_rb->RemoveColumn(index_column_i));

    if (inner)
    {
        for (int i = 0; i < merged_rb->num_columns(); i++)
        {
            if (merged_rb->column(i)->null_count() > 0)
            {
                merged_rb = merged_rb->RemoveColumn(i).MoveValueUnsafe();
            }
        }
    }

    if (ignore_index)
    {
        return merged_rb;
    }
    else
    {
        return { merged_rb, index };
    }
}

using namespace pd;
using namespace string_literals;
using std::pair;
using std::vector;

int main()
{

    auto df1 = pd::DataFrame(NULL_INDEX, pair{ "letter"s, vector{ "a"s, "b"s } }, pair{ "number"s, vector{ 1, 2 } });

    auto df2 = pd::DataFrame(NULL_INDEX, pair{ "letter"s, vector{ "c"s, "d"s } }, pair{ "number"s, vector{ 3, 4 } });

    auto df3 = pd::DataFrame(
        NULL_INDEX,
        pair{ "letter"s, vector{ "c"s, "d"s } },
        pair{ "number"s, vector{ 3, 4 } },
        pair{ "animal"s, vector{ "cat"s, "dog"s } });

    std::cout << joinOnIndex(df1, df2, false) << "\n";

    std::cout << joinOnIndex(df1, df2, true) << "\n";

    std::cout << joinOnIndex(df1, df3, false) << "\n";

    std::cout << joinOnIndex(df1, df3, false, true) << "\n";
    return 0;
}