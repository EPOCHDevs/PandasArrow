#pragma once
//
// Created by dewe on 2/7/24.
//
#include <chrono>
#include <iostream>

template<class UNIT=std::ratio<1>, class T = decltype(std::chrono::high_resolution_clock::now())>
struct TimeIt {

    TimeIt(std::string title) :
    title(std::move(title)),
    start(std::chrono::high_resolution_clock::now()) {}

    ~TimeIt() {
        const T end = std::chrono::high_resolution_clock::now();
        std::cout << title << " took "
                  << std::chrono::duration_cast<std::chrono::duration<int64_t, UNIT>>(end - start).count()
                  << " "
                  << UnitToString() << "\n";
    }

    constexpr const char* UnitToString() {

        if constexpr (std::same_as<UNIT, std::ratio<1>>) {
            return "s";
        } else if constexpr (std::same_as<UNIT, std::milli >) {
            return "ms";
        } else if constexpr (std::same_as<UNIT, std::micro >) {
            return "us";
        }
        else if constexpr (std::same_as<UNIT, std::nano >) {
            return "ns";
        }
        else {
            static_assert(true, "only supports seconds, milliseconds, microseconds, nanoseconds");
        }
    }

    T start;
    std::string title;
};