#pragma once
//
// Created by dewe on 1/1/23.
//

#include "series.h"

namespace pd {
    struct StringLike : public Series{

        class Series ascii_capitalize() const;
        Series ascii_lower() const;
        Series ascii_reverse() const;
        Series ascii_swapcase() const;
        Series ascii_title() const;
        Series ascii_upper() const;
        class Scalar binary_length() const;
        Series binary_repeat() const;
        Series binary_replace_slice(int64_t start, int64_t stop, std::string const &replacement) const;
        Series binary_reverse() const;
        Series replace_substring(std::string const &pattern,
                                 std::string const &replacement,
                                 int64_t const &max_replacements = -1) const;
        Series replace_substring_regex(std::string const &pattern,
                                       std::string const &replacement,
                                       int64_t const &max_replacements = -1) const;
        Series utf8_capitalize() const;
        Series utf8_length() const;
        Series utf8_lower() const;
        Series utf8_reverse() const;
        Series utf8_swapcase() const;
        Series utf8_title() const;
        Series utf8_upper() const;
        Series utf8_replace_slice(int64_t start, int64_t stop,
                                  std::string const &replacement) const;

        Series ascii_is_alnum() const;
        Series ascii_is_alpha() const;
        Series ascii_is_decimal() const;
        Series ascii_is_lower() const;
        Series utf8_is_alnum() const;
        Series utf8_is_alpha() const;
        Series utf8_is_decimal() const;
        Series utf8_is_digit() const;
        Series utf8_is_lower() const;
        Series utf8_is_numeric() const;
        Series utf8_is_printable() const;
        Series utf8_is_space() const;
        Series utf8_is_upper() const;

        Series ascii_is_title() const;
        Series utf8_is_title() const;
        Series string_is_ascii() const;

        Series ascii_center(int64_t width, std::string const& padding = " ") const;
        Series ascii_lpad(int64_t width, std::string const& padding = " ") const;
        Series ascii_rpad(int64_t width, std::string const& padding = " ") const;
        Series utf8_center(int64_t width, std::string const& padding = " ") const;
        Series utf8_lpad(int64_t width, std::string const& padding = " ") const;
        Series utf8_rpad(int64_t width, std::string const& padding = " ") const;

        Series ascii_ltrim(std::string const& characters = " ") const;
        Series ascii_rtrim(std::string const& characters = " ") const;
        Series ascii_trim(std::string const& characters = " ") const;
        Series utf8_ltrim(std::string const& characters = " ") const;
        Series utf8_rtrim(std::string const& characters = " ") const;
        Series utf8_trim(std::string const& characters = " ") const;

        //  Only trim off ASCII whitespace characters ('\t', '\n', '\v', '\f', '\r' and ' ').
        Series ascii_ltrim_whitespace() const;
        Series ascii_rtrim_whitespace() const;
        Series ascii_trim_whitespace() const;

        //  Only trim off Unicode whitespace characters.
        Series utf8_ltrim_whitespace() const;
        Series utf8_rtrim_whitespace() const;
        Series utf8_trim_whitespace() const;

        Series utf8_slice_codeunits(int64_t start,
                                    int64_t stop = std::numeric_limits<int64_t>::max(),
                                    int64_t step = 1) const;

        Series ascii_split_whitespace(int64_t max_splits = -1, bool reverse = false) const;
        Series split_pattern(std::string const& pattern, int64_t max_splits = -1, bool reverse = false) const;
        Series split_pattern_regex(std::string const& pattern, int64_t max_splits = -1, bool reverse = false) const;
        Series utf8_split_whitespace(int64_t max_splits = -1, bool reverse = false) const;

        Series extract_regex(std::string const& pattern) const;

        Series binary_join(Series const& joiner) const;
        Series binary_join(Scalar const& joiner) const;

        Series count_substring(std::string const& pattern, bool ignore_case = false);
        Series count_substring_regex(std::string const& pattern, bool ignore_case = false);
        Series ends_with(std::string const& pattern, bool ignore_case = false);
        Series find_substring(std::string const& pattern, bool ignore_case = false);

        Series index_in(Series const& value_set, bool skip_nulls = false);
        Series is_in(Series const& value_set, bool skip_nulls = false);

        Series match_like(std::string const& pattern, bool ignore_case = false);
        Series match_substring(std::string const& pattern, bool ignore_case = false);
        Series match_substring_regex(std::string const& pattern, bool ignore_case = false);
        Series starts_with(std::string const& pattern, bool ignore_case = false);

    private:
        friend Series;

        StringLike(const std::shared_ptr<arrow::Array> &array) :
              Series(array, nullptr, "", true) {}
    };
}