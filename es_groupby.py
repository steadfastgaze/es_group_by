import pandas as pd
from pandas.io.json import json_normalize


class EsGroupBy:

    def __init__(self,
                 es_connection,
                 index_pattern,
                 time_range_start,
                 time_range_end,
                 filters,
                 single_page_size=10000,
                 groupbys=None,
                 operations=None):

        self.es = es_connection
        self.SIZE = 10000
        self.index_pattern = index_pattern
        self.groupby(groupbys)
        self.agg(operations)
        self.filters = filters
        self.time_range_start = time_range_start
        self.time_range_end = time_range_end
        self.dataframe = pd.DataFrame()

    def groupby(self, groupby_list):
        self.groupby_list = [groupby_list] if isinstance(
            groupby_list, str) else groupby_list
        return self

    def agg(self, operations_list):
        self.operations_list = [
            {k: v} for k, v in operations_list.items()
        ] if isinstance(operations_list, dict) else operations_list

    def __sources_element_builder(self, name):
        return {
            name: {
                'terms': {
                    'field': name,
                    'order': 'asc'
                }
            }
        }

    def __sources_builder(self, groupby_list):
        sources = []

        for el in groupby_list:
            sources.append(self.__sources_element_builder(el))

        return sources

    def __aggregations_element_builder(self, field_operation):
        field, operation = next(iter(field_operation.items()))

        return {
            field + '_' + operation: {
                operation: {
                    'field': field
                }
            }}

    def __aggregations_builder(self, operations_list):
        operations = {}

        for el in operations_list:
            operations.update(self.__aggregations_element_builder(el))

        return operations

    def __filter_element_builder(self, field_value):
        field, value = next(iter(field_value.items()))

        return {
            'match_phrase': {
                field: {
                    'query': value
                }
            }
        }

    def __time_range_filter_builder(self, start, end):
        return {
            'range': {
                '@timestamp': {
                    'from': start,
                    'to': end,
                    'include_lower': True,
                    'include_upper': False
                }
            }
        }

    def __filters_builder(self, filters, time_range_start, time_range_end):
        filters_value = []

        for el in filters:
            filters_value.append(self.__filter_element_builder(el))

        filters_value.append(self.__time_range_filter_builder(
            time_range_start,
            time_range_end))

        return filters_value

    def dsl(self, after=None):

        if after is None:
            composite_value = {"size": self.SIZE,
                               "sources": self.__sources_builder(
                                   self.groupby_list)}
        else:
            composite_value = {"size": self.SIZE,
                               "sources": self.__sources_builder(
                                   self.groupby_list),
                               "after": after}

        my_buckets_value = {"composite": composite_value,
                            "aggregations": self.__aggregations_builder(
                                self.operations_list)}

        aggs_value = {"my_buckets": my_buckets_value}

        must_value = self.__filters_builder(self.filters,
                                            self.time_range_start,
                                            self.time_range_end)

        query_value = {
            "bool": {
                "must": must_value,
                "filter": [
                    {
                        "match_all": {}
                    }
                ]
            }
        }

        # "size" : 0 because here hits are not needed, just aggs
        full_dsl = {"size": 0, "aggs": aggs_value, "query": query_value}

        return full_dsl

    def execute(self):
        num_iteration = 0
        after_key = None
        result_size = -1

        while(result_size == -1 or result_size == self.SIZE):
            dsl = self.dsl(after_key)

            res_json = self.es.search(
                index=self.index_pattern,
                body=dsl
            )

            res_json_buckets = res_json['aggregations'][
                'my_buckets'][
                'buckets']

            after_key = res_json['aggregations'][
                'my_buckets'][
                'after_key']

            df_res = pd.DataFrame(res_json_buckets)
            df_list = [json_normalize(df_res['key'])]
            df_list.append(df_res['doc_count'])

            for el in self.operations_list:
                field, value = next(iter(el.items()))
                field_name = field+'_'+value
                df_op_result = json_normalize(df_res[field_name]).rename(
                    columns={'value': field_name})
                df_list.append(df_op_result)

            df_prep = pd.concat(df_list, axis=1)

            self.dataframe = self.dataframe.append(df_prep)

            result_size = df_prep.shape[0]

            num_iteration = num_iteration + 1
            print('Iteration: ' + str(num_iteration))
            print('Last result size: ' + str(result_size))
            print('Last keys: ' + str(after_key))

        return self
