# Panda-Apache
Panda-Apache is a C++ library for working with data in Apache Arrow format. It is built on top of the Apache Arrow library, and provides a convenient, high-level interface for working with data in memory.

### Features
* DataFrame: A DataFrame is a 2-dimensional table of data with rows and columns. It is similar to a pandas DataFrame or a R dataframe. It provides the following functionality:
    * Indexing by label and by position
Slicing rows and columns
Selecting rows and columns by conditions
Handling of missing values
Groupby operations
Merging and joining DataFrames
Reshaping and pivoting
Time series functionality
I/O with different file formats
Series: A Series is a 1-dimensional array of data with an index. It is similar to a pandas Series or a R vector. It provides the following functionality:
Indexing by label and by position
Handling of missing values
Mathematical operations
String operations
Date-time operations
Frequency counting
NDArray: A NDArray is a multi-dimensional array of data. It is similar to a numpy ndarray. It provides the following functionality:
Indexing by position
Mathematical operations
Broadcasting
Scalar: A Scalar is a single value. It provides the following functionality:
Mathematical operations
String operations
Date-time operations
