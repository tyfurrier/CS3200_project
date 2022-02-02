
class Aggs:
    """Holds constant string representations for the supported aggregation methods of numerical features
    as of Jan 28, 2022 ... DC = distinct count (excluding duplicates) DCE = distinct count estimate
    NDC = Non-Distinct Count ..."""
    SUM = 'SUM'
    AVG = 'AVG'
    MAX = 'MAX'
    MIN = 'MIN'
    DISTINCT_COUNT = 'DC'
    DISTINCT_COUNT_ESTIMATE = 'DCE'
    NON_DISTINCT_COUNT = 'NDC'
    STDDEV_SAMP = 'STDDEV_SAMP'
    STDDEV_POP = 'STDDEV_POP'
    VAR_SAMP = 'VAR_SAMP'
    VAR_POP = 'VAR_POP'
