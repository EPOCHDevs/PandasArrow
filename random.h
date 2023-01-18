#pragma once
//
// Created by dewe on 1/9/23.
//
#include <algorithm>
#include <random>
#include "optional"
#include "ranges"
#include "vector"


namespace pd::random {

using vector_double = std::vector<double>;
using vector_int = std::vector<int64_t>;

class RandomState
{
public:
    explicit RandomState(std::optional<uint64_t> const& seed)
        : gen(seed ? *seed : std::random_device()())
    {
    }

    inline void rand(vector_double& result, double min = 0, double max = 1)
        const
    {
        std::ranges::generate(
            result,
            [min, max, this]()
            {
                std::uniform_real_distribution dist(min, max);
                return dist(gen);
            });
    }

    inline vector_double rand(int size, double min = 0, double max = 1) const
    {
        vector_double result(size);
        rand(result, min, max);
        return result;
    }

    inline vector_double rand(
        vector_double const& min,
        vector_double const& max) const
    {
        vector_double result(min.size());
        std::transform(
            min.begin(),
            min.end(),
            max.begin(),
            std::begin(result),
            [&](double lb, double ub)
            {
                std::uniform_real_distribution<double> rand(lb, ub);
                return rand(gen);
            });
        return result;
    }

    inline void randn(vector_double& result, double mean = 0, double std = 1)
        const
    {
        std::ranges::generate(
            result,
            [mean, std, this]()
            {
                std::normal_distribution dist(mean, std);
                return dist(gen);
            });
    }

    inline vector_double randn(int size, double mean = 0, double std = 1) const
    {
        vector_double result(size);
        randn(result, mean, std);
        return result;
    }

    inline void randint(vector_int& result, int min = 0, int max = 1) const
    {
        std::ranges::generate(
            result,
            [min, max, this]()
            {
                std::uniform_int_distribution dist(min, max);
                return dist(gen);
            });
    }

    inline vector_int randint(int size, int min = 0, int max = 1) const
    {
        vector_int result(size);
        randint(result, min, max);
        return result;
    }

    template<class T>
    static std::shared_ptr<arrow::Array> toArray(std::vector<T> const& v)
    {
        typename arrow::CTypeTraits<T>::BuilderType b;
        b.AppendValues(v);
        return b.Finish().ValueOrDie();
    }

    private:
    mutable std::mt19937_64 gen;
};
}