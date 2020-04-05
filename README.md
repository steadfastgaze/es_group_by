
[![License: CC0-1.0](https://licensebuttons.net/p/zero/1.0/88x31.png)](http://creativecommons.org/publicdomain/zero/1.0/)


# Elastic GroupBy to Pandas DataFrame (Composite Aggregation)
This is a example of Python class that does a "GROUP BY" on Elasticsearch and returns a Python Pandas dataframe, and also automatically manages pagination (more than 10000 results with default settings).


## Basic usage
```python
gb = EsGroupBy(es,
              index_pattern='whatever-*',
              time_range_start='2020-01-01',
              time_range_end='2020-01-02',
              filters=[{'field_to_filter.keyword': 'mario'}])

gb.groupby('account.keyword').agg({'euro': 'sum'})

df = gb.execute().dataframe
```

## groupby method also accepts multiple fields:
```python
gb.groupby(['account.keyword', 'hello.keyword'])
```
## agg method also accept multiple fields, in two different ways:
```python
gb.agg({'euro': 'sum','dollars': 'avg'})
gb.agg([{'euro': 'sum'},{'dollars': 'avg'}])
```
If you need to do two aggregations on same field (es. avg and sum of field 'euro'), to respect key value associations you need the second syntax.

## Disclaimer
I'm still learning Python.