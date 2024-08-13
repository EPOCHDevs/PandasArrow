//
// Created by adesola on 8/13/24.
//
#include "list_s3_files.h"
#include <iostream>


int main()
{
    auto files = pd::ListS3Paths("s3://epoch-db/MinuteBars/Futures/ES");
    for (const auto &object : files)
    {
        std::cout << " - " << object << std::endl;
    }
    return 0;
}