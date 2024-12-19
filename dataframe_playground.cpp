//
// Created by adesola on 12/18/24.
//
#include <DataFrame/DataFrame.h>
#include <pandas_arrow.h>
struct  MyData  {
    int         i { 10 };
    double      d { 5.5 };
    std::string s { "Some Arbitrary String" };

};

using namespace hmdf;
using LDataFrame = hmdf::StdDataFrame<int64_t>;
LDataFrame createDataFrameFromArrow(const pd::DataFrame& record_batch) {
    LDataFrame df;

    df.load_index(record_batch.index().values<int64_t>());
    for(const auto& column: record_batch.columns())
    {
        df.load_column(column.c_str(), record_batch[column].values<double>());
    }
    return df;
}

int main() {
    std::vector<unsigned long> idx_col1 = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    std::vector<int> int_col1 = {1, 2, -3, -4, 5, 6, 7, 8, 9, -10};
    std::vector<double> dbl_col1 = {0.01, 0.02, 0.03, 0.03, 0.05, 0.06, 0.03, 0.08, 0.09, 0.03};

    pd::DataFrame df{
            pd::range(0L, 10),
            pd::GetRow("i", int_col1),
            pd::GetRow("i1", int_col1),
            pd::GetRow("i2", dbl_col1),
            pd::GetRow("i3", dbl_col1),
            pd::GetRow("i4", ranges::to<std::vector>(dbl_col1 | std::views::reverse))
    };

    StationaryCheckVisitor<double, int64_t> sc3{stationary_test::adf, {.adf_lag = 25, .adf_with_trend = false}};

    auto v = df["i2"].hmVisit(sc3);

    std::cout << "adf_statistic: " << v.get_adf_statistic() << "\n";
    std::cout << "kpss_statistic: " << v.get_kpss_statistic() << "\n";
    std::cout << "kpss_value: " << v.get_kpss_value() << "\n";

    CorrVisitor<double> s_corr_visitor(correlation_type::spearman);
    auto corr_result = df.hmVisit(s_corr_visitor, "i2", "i3");
    std::cout << "Spearman correlation Corrolating: " << corr_result.get_result() << "\n";


    auto _corr_result = df.hmVisit(s_corr_visitor, "i3", "i4");
    std::cout << "Spearman correlation Non-Corrolating: " << _corr_result.get_result() << "\n";
    return 0;
}