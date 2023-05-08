//
// Created by dewe on 1/18/23.
//

#include "pandas_arrow.h"


int main()
{
    using namespace std::string_literals;
    using namespace pd;

    TableLike<int64_t> data;

    auto random = pd::random::RandomState(1);
    for (int i = 0; i < 400; i++)
    {
        data[std::to_string(i)] = random.randint(1e6, 10, 20);
    }

    auto df = pd::DataFrame(data, range(0l, 1e6l));

    auto start = std::chrono::high_resolution_clock::now();

    auto result = (((df + df) * df) / df).sum();

    auto end = std::chrono::high_resolution_clock::now();

    std::cout << "elapsed time(s): " << std::chrono::duration<double>(end - start).count() << " s.\n";

    return 0;
}