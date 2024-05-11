//
// Created by adesola on 5/10/24.
//

#pragma once
#include <memory>
#include <arrow/io/api.h>
#include "arrow/filesystem/s3fs.h"


namespace arrow {

    class AWSS3Reader
    {
    public:
        static AWSS3Reader& Instance() {
            static AWSS3Reader instance;
            return instance;
        }

        auto CreateReadableFile(std::string const& s3URI) const
        {
            return m_s3fs->OpenInputFile(s3URI);
        }

        ~AWSS3Reader();

    private:
        AWSS3Reader();

        std::shared_ptr<fs::FileSystem> m_s3fs;
    };
    }
