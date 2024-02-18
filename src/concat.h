#pragma once
//
// Created by dewe on 1/22/23.
//

#include "concat.h"
#include "core.h"
#include "dataframe.h"
#include "series.h"


namespace pd {

using NDFrameType = std::variant<pd::DataFrame, pd::Series>;
struct Concatenator
{

    Concatenator(
        std::vector<pd::DataFrame> const& objs,
        JoinType join = JoinType::Outer,
        bool ignore_index = false,
        bool sort = false);

    inline pd::DataFrame operator()(AxisType axis_type)
    {
        if (axis_type == AxisType::Index)
        {
            return concatenateRows(intersect, ignore_index, sort);
        }
        else
        {
            return concatenateColumns(intersect, ignore_index, sort);
        }
    }

    static arrow::ArrayVector makeJoinIndexes(std::vector<pd::DataFrame> const& objs, AxisType axis);

    pd::DataFrame concatenateRows(bool intersect, bool ignore_index, bool sort);

    pd::DataFrame concatenateColumns(bool intersect, bool ignore_index, bool sort);

    static ArrayPtr mergeIndexes(arrow::ArrayVector const& indexes, bool intersect);

    static ArrayPtr makeConcatIndex(std::vector<pd::DataFrame> const& objs, AxisType axis, bool ignoreIndex);

    static std::unordered_map<std::string, std::pair<std::vector<int>, std::shared_ptr<arrow::DataType>>>
    resolveDuplicateFieldName(std::vector<pd::DataFrame> const& objs);

private:
    std::vector<pd::DataFrame> objs{};
    bool intersect{};
    bool ignore_index{};
    bool sort{};
};

inline DataFrame concat(
    std::vector<pd::DataFrame> const& objs,
    AxisType axis = AxisType::Index,
    JoinType join = JoinType::Outer,
    bool ignore_index = false,
    bool sort = false)
{
    return Concatenator(objs, join, ignore_index, sort)(axis);
}

// inline pd::DataFrame concat(
//    std::vector<pd::Series> const& objs,
//    AxisType axis = AxisType::Index,
//    JoinType join = JoinType::Outer,
//    bool ignore_index = false,
//    bool sort = false)
//{
//    // create a vector of DataFrames from the Series
//    std::vector<pd::DataFrame> dfs;
//    int i = 0;
//    for (auto const& obj : objs)
//    {
//        auto name = axis == AxisType::Index ?
//            "" :
//            (obj.name().empty() ? std::to_string(i++) : obj.name());
//        dfs.push_back(obj.toFrame(name));
//    }
//    // call the existing DataFrame concatenation function
//    return Concatenator(dfs, join, ignore_index, sort)(axis);
//}

// inline pd::DataFrame concat(
//    std::vector<std::variant<pd::DataFrame, pd::Series>> const& objs,
//    AxisType axis = AxisType::Index,
//    JoinType join = JoinType::Outer,
//    bool ignore_index = false,
//    bool sort = false) {
//    // create a vector of DataFrames from the variant objects
//    int i = 0;
//    std::vector<pd::DataFrame> dfs;
//    dfs.emplace_back();
//    std::vector<pd::Series> series;
//
//    for (auto const& obj : objs) {
//        if (auto df = std::get_if<pd::DataFrame>(&obj)) {
//            dfs.push_back(*df);
//        } else {
//            auto s = std::get<pd::Series>(obj);
//            series.emplace_back(s);
//        }
//    }
//
//    if(series.empty())
//    {
//        dfs = std::vector(dfs.begin()+1, dfs.end());
//    }
//    else
//    {
//        dfs[0] = concat(series, axis, join,ignore_index,sort);
//        if(axis == AxisType::Index)
//        {
//            dfs[0] = dfs[0].rename({ { "", "0" } });
//        }
//    }
//    // call the existing DataFrame concatenation function
//    return Concatenator(dfs, join, ignore_index, sort)(axis);
//}

DataFrame concatColumnsUnsafe(std::vector<pd::DataFrame> const& objs);

} // namespace pd