# Panda-Apache
Panda-Apache is a C++ library for working with data in Apache Arrow format. It is built on top of the Apache Arrow library, and provides a convenient, high-level interface for working with data in memory.

### Features
* DataFrame: A DataFrame is a 2-dimensional table of data with rows and columns. It is similar to a pandas DataFrame or a R dataframe. It provides the following functionality:
    * Indexing by label and by position
    * Slicing rows and columns
    * Selecting rows and columns by conditions
    * Handling of missing values
    * Groupby operations
    * Merging and joining DataFrames
    * Reshaping and pivoting
    * Time series functionality
    * I/O with different file formats
* Series: A Series is a 1-dimensional array of data with an index. It is similar to a pandas Series or a R vector. It provides the following functionality:
    * Indexing by label and by position
    * Handling of missing values
    * Mathematical operations
    * String operations
    * Date-time operations
    * Frequency counting
* Scalar: A Scalar is a single value. It provides the following functionality:
    * Mathematical operations

### Installation

Panda-Apache is available on GitHub and can be installed with CMake. It requires a C++11 compiler and the Apache Arrow library to be installed.

```
git clone https://github.com/ava6969/panda-arrow.git
cd pandas-cpp
mkdir build
cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
make
```

### Usage

Here is an example of how to use Panda-Apache to create a DataFrame and perform some operations on it:

```
#include <panda_arrow.h>
#include <iostream>

int main() {
    // Create a DataFrame from a map of column names to vectors of data
    auto df = pd::DataFrame(
        std::map<std::string, std::vector<int32_t>>{{"a", {1, 2, 3}}, {"b", {4, 5, 6}}});

    // Print the DataFrame
    std::cout << df << std::endl;

    // Get the value at a specific row and column
    auto value = df.at(1, "a").value<int32_t>();
    std::cout << "Value at (1, 'a'): " << value << std::endl;

    // Get a slice of the DataFrame
    auto df2 = df.slice(1);
    std::cout << "DataFrame slice:" << std::endl << df2 << std::endl;

    // Rename columns
    df.rename({{"a", "x"}, {"b", "y"}});
    std::cout << "DataFrame with renamed columns:" << std::endl << df << std::endl;

    return 0;
}

```

### Renaming Columns

You can use the rename() method to change the names of one or multiple columns. This method takes a std::unordered_map where the key is the current column name and the value is the new column name.

```
pd::DataFrame df = pd::DataFrame(
    std::map<std::string, std::vector<int32_t>>{{"a", {1, 2, 3}}, {"b", {4, 5, 6}}, {"c", {7, 8, 9}}});
df.rename({{"a", "x"},{"b", "y"},{"c", "z"}});

```

You can also use the add_prefix() and add_suffix() methods to add a prefix or suffix to all column names respectively.

```

pd::DataFrame df = pd::DataFrame(
    std::map<std::string, std::vector<int32_t>>{{"a", {1, 2, 3}}, {"b", {4, 5, 6}}, {"c", {7, 8, 9}}});
df.add_prefix("col_");
df.add_suffix("_data");

```

### Series
A Series is a single column of a DataFrame. You can select a single column of a DataFrame by using the operator[] with a single column label.

```
pd::DataFrame df = pd::DataFrame(
    std::map<std::string, std::vector<int32_t>>{{"a", {1, 2, 3}}, {"b", {4, 5, 6}}, {"c", {7, 8, 9}}});
pd::Series col_a = df["a"];

```

### Slicing
You can use the slice() method to select a subset of rows from a DataFrame. This method takes an integer offset and returns a new DataFrame containing the rows starting from that offset.

### API Reference
The full API reference for Panda-Apache can be found here.

### Contributing
We welcome contributions to Panda-Apache. Please submit a pull request with your changes and we will review them as soon as possible.

### License
Panda-Apache is licensed under the Apache 2.0 license. See LICENSE for more information.

### Special THanks to OpenAI ChatGPT for generating this doc and assisted in test cases
