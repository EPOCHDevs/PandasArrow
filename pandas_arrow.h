#pragma once
//
// Created by dewe on 12/29/22.
//

#include "concat.h"
#include "core.h"
#include "datetimelike.h"
#include "group_by.h"
#include "resample.h"
#include "stringlike.h"
#include "cudf/dataframe.h"


namespace pd {
template<class V>
using TableLike = std::map<std::string, std::vector<V>>;
}