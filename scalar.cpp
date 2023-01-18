//
// Created by dewe on 12/28/22.
//
#include "filesystem"
#include <iostream>
#include "scalar.h"


namespace pd
{
    Scalar::Scalar(std::shared_ptr<arrow::Scalar> &&value):scalar(std::move(value)) {}

}