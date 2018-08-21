#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 8/18/2018 5:55 PM
# @Author  : Ruiming_Ma
# @Site    : 
# @File    : ElasticSearch.py
# @Software: PyCharm Community Edition

from elasticsearch import Elasticsearch
import json

es = Elasticsearch()

dsl = {
    'query': {
        'match_all': {
            #'type': '64'  #用在 match 中，现在使用 match_all 就不再添加这个条件了
        }
    },
    'aggs': {
        'all_comments': {
            'terms': {
                'field': 'content',
                #'order': {'_count' : 'desc'}
            }
        },
    }
}

result = es.search(index='taobao', doc_type='iPhone', body=dsl)
print(json.dumps(result, indent=2, ensure_ascii=False))