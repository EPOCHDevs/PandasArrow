#pragma once
//
// Created by dewe on 2/12/24.
//
#include <spdlog/spdlog.h>

#include "scalar.h"
#include "series.h"
#include "dataframe.h"


namespace pd
{
struct DataVariant
{
    const std::variant<DataFrame, Series, Scalar> impl;

    DataFrame GetDataFrame() const
    {
        try
        {
            return std::get<DataFrame>(impl);
        }
        catch (std::bad_variant_access const&)
        {
            SPDLOG_ERROR("Tried accessing DataFrame from variant containing {}.",
                (impl.index() == 1 ? "Series" : "Scalar"));
            throw;
        }
    }

    Series GetSeries() const {
        try
        {
            return std::get<Series>(impl);
        }
        catch (std::bad_variant_access const&)
        {
            SPDLOG_ERROR("Tried accessing Series from variant containing {}.",
                (impl.index() == 0 ? "DataFrame" : "Scalar"));
            throw;
        }
    }

    Scalar GetScalar() const {
        try
        {
            return std::get<Scalar>(impl);
        }
        catch (std::bad_variant_access const&)
        {
            SPDLOG_ERROR("Tried accessing Scalar from variant containing {}.",
                (impl.index() == 0 ? "DataFrame" : "Series"));
            throw;
        }
    }
};
}