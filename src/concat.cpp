//
// Created by dewe on 1/22/23.
//
#include <climits>
#include "concat.h"
#include <tbb/parallel_for.h>
#include "dataframe.h"
#include "series.h"


namespace pd {
Concatenator::Concatenator(const std::vector<pd::DataFrame>& objs, JoinType join, bool ignore_index, bool sort)
    : ignore_index(ignore_index), sort(sort)
{
    if (objs.empty())
    {
        throw std::invalid_argument("No objects to concatenate");
    }

    if (join == JoinType::Outer)
    {
        intersect = false;
    }
    else if (join == JoinType::Inner)
    {
        intersect = true;
    }
    else
    {
        throw std::invalid_argument("Only can inner (intersect) or outer (union) join the other axis");
    }

    this->objs.reserve(objs.size());
    std::ranges::copy_if(
        objs,
        std::back_inserter(this->objs),
        [](const pd::DataFrame& df) { return df.m_array != nullptr; });}

arrow::ArrayVector Concatenator::makeJoinIndexes(std::vector<pd::DataFrame> const& objs, AxisType concat_axis)
{
    auto selectIndex = concat_axis == AxisType::Columns ? [](pd::DataFrame const& obj) { return obj.indexArray(); } :
                                                          [](pd::DataFrame const& obj) { return obj.fieldArray(); };

    std::vector<std::shared_ptr<arrow::Array>> indexes(objs.size());
    std::ranges::transform(objs, indexes.begin(), selectIndex);
    return indexes;
}

ArrayPtr Concatenator::makeConcatIndex(std::vector<pd::DataFrame> const& objs, AxisType axis, bool ignoreIndex)
{
    auto selectIndex = axis == AxisType::Columns ? [](pd::DataFrame const& obj) { return obj.fieldArray(); } :
                                                   [](pd::DataFrame const& obj) { return obj.indexArray(); };

    if (ignoreIndex)
    {
        auto getSize = [selectIndex](size_t total, pd::DataFrame const& obj)
        { return selectIndex(obj)->length() + total; };

        ::uint64_t totalSize = std::accumulate(objs.begin(), objs.end(), 0UL, getSize);

        ArrayPtr newIndex = pd::range(0UL, totalSize);
        if (axis == AxisType::Columns)
        {
            return arrow::compute::Cast(newIndex, arrow::utf8())->make_array();
        }
        return newIndex;
    }
    else
    {
        arrow::ArrayVector indexes;

        std::ranges::transform(objs, std::back_inserter(indexes), selectIndex);
        auto result = ReturnOrThrowOnFailure(arrow::Concatenate(indexes));
        return result;
    }
}

ArrayPtr Concatenator::mergeIndexes(arrow::ArrayVector const& indexes, bool intersect)
{
    auto index = indexes[0];
    for (auto other = indexes.begin() + 1; other != indexes.end(); other++)
    {
        auto lhs = Series(index, true);
        auto rhs = Series{ *other, true };
        index = (intersect ? lhs.intersection(rhs) : lhs.union_(rhs)).m_array;
    }
    return index;
}

std::unordered_map<std::string, std::pair<std::vector<int>, std::shared_ptr<arrow::DataType>>> Concatenator::
    resolveDuplicateFieldName(std::vector<pd::DataFrame> const& objs)
{
    std::unordered_map<std::string, std::pair<std::vector<int>, std::shared_ptr<arrow::DataType>>> result;

    int i = 0;
    for (auto const& obj : objs)
    {
        for (auto const& field : obj.array()->schema()->fields())
        {
            const std::string fieldName = field->name();
            if (result.count(fieldName) == 0)
            {
                result[fieldName] = std::pair{ std::vector{ i }, field->type() };
            }
            else
            {
                result[fieldName].first.push_back(i);
                result[fieldName].second = promoteTypes({ result[fieldName].second, field->type() });
            }
        }
        i++;
    }
    return result;
}

pd::DataFrame Concatenator::concatenateRows(bool intersect, bool ignore_index, bool /*sort*/)
{

    auto newDataTypes = resolveDuplicateFieldName(objs);
    for (auto const& [field, value] : newDataTypes)
    {
        if (value.first.size() > 1)
        {
            auto const& new_type = value.second;
            for (auto const& idx : value.first)
            {
                auto recordBatch = objs.at(idx).array();
                auto field_index = recordBatch->schema()->GetFieldIndex(field);
                auto casted = arrow::compute::Cast(recordBatch->column(field_index), new_type)->make_array();
                objs.at(idx) =
                    DataFrame{ pd::ReturnOrThrowOnFailure(
                                   recordBatch->SetColumn(field_index, arrow::field(field, new_type), casted)),
                               objs.at(idx).indexArray() };
            }
        }
    }

    // Convert DataFrames to tables
    auto unique_index = std::to_string(std::chrono::high_resolution_clock::now().time_since_epoch().count()) + "index";
    std::vector<TablePtr> tables(objs.size());

    std::ranges::transform(
        objs,
        tables.begin(),
        [&unique_index](auto const& obj) { return obj.toTable(unique_index); });

    // Define merge options for columns in case of name conflicts
    arrow::Field::MergeOptions merge_options;
    merge_options.promote_nullability = true; // Rename conflicting columns

    // Concatenate tables
    arrow::ConcatenateTablesOptions options;
    options.unify_schemas = true; // Automatically merge schemas of tables
    options.field_merge_options = merge_options; // Use defined merge options for columns

    auto table = pd::ReturnOrThrowOnFailure(arrow::ConcatenateTables(tables, options));

    auto merged_rb = pd::ReturnOrThrowOnFailure(table->CombineChunksToBatch());

    auto index_column_i = merged_rb->schema()->GetFieldIndex(unique_index);
    auto index = merged_rb->column(index_column_i);

    merged_rb = pd::ReturnOrThrowOnFailure(merged_rb->RemoveColumn(index_column_i));

    //    if (sort)
    //    {
    //    auto newFieldNames = dynamic_pointer_cast<arrow::StringArray>(
    //        mergeIndexes(makeJoinIndexes(objs, AxisType::Index), intersect));
    //    const size_t numColumns = newFieldNames->length();
    //        auto sort_indices = ReturnOrThrowOnFailure(
    //            arrow::compute::SortIndices(
    //                newFieldNames,
    //                arrow::compute::SortOptions{}));
    //
    //        newFieldNames = arrow::compute::Take(newFieldNames, sort_indices)
    //                            ->array_as<arrow::StringArray>();
    //    }

    if (intersect)
    {
        auto column_names = merged_rb->schema()->field_names();
        for (auto const& name : column_names)
        {
            auto i = merged_rb->schema()->GetFieldIndex(name);
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

pd::DataFrame Concatenator::concatenateColumns(bool intersect, bool ignore_index, bool sort)
{
    auto newIndexes = mergeIndexes(makeJoinIndexes(objs, AxisType::Columns), intersect);
    const size_t numRows = newIndexes->length();

    if (sort)
    {
        auto sort_indices =
            ReturnOrThrowOnFailure(arrow::compute::SortIndices(newIndexes, arrow::compute::SortOptions{}));

        newIndexes = arrow::compute::Take(newIndexes, sort_indices)->make_array();
    }


    std::vector<size_t> index_offset;
    index_offset.reserve(objs.size());
    auto newColumnLength = accumulate(
        objs.begin(),
        objs.end(),
        0UL,
        [&index_offset](size_t total, DataFrame const& df)
        {
            index_offset.push_back(total);
            return total + df.num_columns();
        });

    arrow::FieldVector fieldVector(newColumnLength);
    arrow::ArrayDataVector arrayVectors(newColumnLength);

    tbb::parallel_for(
        0UL,
        objs.size(),
        [&](size_t i)
        {
            auto& obj = objs[i];
            if (!obj.indexArray()->Equals(newIndexes))
            {
                obj = obj.reindexAsync(newIndexes);
            }
            std::ranges::copy(obj.array()->schema()->fields(), fieldVector.begin() + index_offset[i]);
            std::ranges::copy(obj.array()->column_data(), arrayVectors.begin() + index_offset[i]);
        });

    if (ignore_index)
    {
        for (size_t i = 0; i < fieldVector.size(); i++)
        {
            fieldVector.at(i) = arrow::field(std::to_string(i), fieldVector.at(i)->type());
        }
    }

    return { arrow::schema(fieldVector), static_cast<int64_t>(numRows), arrayVectors, newIndexes };
}

pd::DataFrame concatColumnsUnsafe(std::vector<pd::DataFrame> const& objs)
{
    auto df = objs.at(0).array();
    auto N = df->num_columns();

    for (size_t i = 1; i < objs.size(); i++)
    {
        for (auto const& field : objs[i].array()->schema()->fields())
        {
            auto result = df->AddColumn(N++, field, objs[i][field->name()].m_array);
            if (result.ok())
            {
                df = result.MoveValueUnsafe();
            }else
            {
                std::cerr << df->ToString()
                          << "\nLength: " << df->num_rows() << "\nNewColumn: " << objs[i][field->name()].m_array->ToString()
                          << "\nNew Column Length: " << objs[i][field->name()].m_array->length()
                          << "\nField: " << field->name() << "\n";
                throw std::runtime_error(result.status().ToString());
            }
        }
    }
    return { df, objs.at(0).indexArray() };
}

} // namespace pd