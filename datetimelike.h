#pragma once
//
// Created by dewe on 1/1/23.
//
#include "series.h"


namespace pd{

    struct DateTimeLike : public Series{

        Series ceil(int multiple = 1, arrow::compute::CalendarUnit unit = arrow::compute::CalendarUnit::DAY,
                    bool week_starts_monday = true,
                    bool ceil_is_strictly_greater = false,
                    bool calendar_based_origin = false) const;

        Series floor(int multiple = 1, arrow::compute::CalendarUnit unit = arrow::compute::CalendarUnit::DAY,
                    bool week_starts_monday = true,
                    bool ceil_is_strictly_greater = false,
                    bool calendar_based_origin = false) const;

        Series round(int multiple = 1, arrow::compute::CalendarUnit unit = arrow::compute::CalendarUnit::DAY,
                    bool week_starts_monday = true,
                    bool ceil_is_strictly_greater = false,
                    bool calendar_based_origin = false) const;

        // Temporal component extraction

        Series day() const;
        Series day_of_week() const;
        Series day_of_year() const;
        Series hour() const;
        Series is_dst() const;
        Series iso_week() const;
        Series iso_year() const;
        DataFrame iso_calendar() const;
        Series is_leap_year() const;
        Series microsecond() const;
        Series millisecond() const;
        Series minute() const;
        Series month() const;
        Series nanosecond() const;
        Series quarter() const;
        Series second() const;
        Series subsecond() const;
        Series us_week() const;
        Series us_year() const;
        Series week(bool week_starts_monday = true,
                    bool count_from_zero = false,
                    bool first_week_is_fully_in_year = false) const;
        Series year() const;
        DataFrame year_month_day() const;

        Series day_time_interval_between(Series const& other) const;
        Series days_between(Series const& other) const;
        Series hours_between(Series const& other) const;
        Series microseconds_between(Series const& other) const;
        Series milliseconds_between(Series const& other) const;
        Series minutes_between(Series const& other) const;
        Series month_day_nano_interval_between(Series const& other) const;
        Series month_interval_between(Series const& other) const;
        Series nanoseconds_between(Series const& other) const;
        Series quarters_between(Series const& other) const;
        Series seconds_between(Series const& other) const;
        Series weeks_between(Series const& other) const;
        Series years_between(Series const& other) const;

    private:
        friend Series;

        DateTimeLike(const std::shared_ptr<arrow::Array> &array) :
              Series(array, nullptr, "", true) {}
    };
}