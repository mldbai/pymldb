# -*- coding: utf-8 -*-
# Copyright (c) 2015 Datacratic Inc.  All rights reserved.
# @Author:             Alexis Tremblay
# @Email:              atremblay@datacratic.com
# @Date:               2015-03-19 10:28:23
# @Last Modified by:   Alexis Tremblay
# @Last Modified time: 2015-04-21 09:05:54
# @File Name:          index.py

from pymldb.query import Query


class Time(object):
    """docstring for Time"""
    def __init__(self, dataset_url):
        super(Time, self).__init__()
        self.dataset_url = dataset_url
        self.query = Query(dataset_url)

    def __getitem__(self, value):
        print(value)

    def copy(self):
        copy_time = Time(self.dataset_url)
        copy_time = self.query.copy()
        return copy_time


class Index(object):
    """docstring for Index"""
    def __init__(self, bf):
        super(Index, self).__init__()
        self._bf = bf

    def copy(self):
        copy_index = Index(self._bf.copy())
        return copy_index

    def __getitem__(self, val):
        if isinstance(val, str):
            copy_bf = self._bf.copy()
            copy_bf.query.addWHERE("rowName()='{}'".format(val))
            return copy_bf
        elif isinstance(val, list):
            copy_bf = self._bf.copy()
            where = []
            for v in val:
                where.append("rowName()='{}'".format(v))

            copy_bf.query.addWHERE("({})".format(" OR ".join(where)))
            return copy_bf
        elif isinstance(val, tuple):
            if len(val) != 2:
                msg = "Too many indexers"
                raise IndexError(msg)

            row_index = val[0]
            col_index = val[1]
            row_index_bf = self._bf.ix[row_index]
            col_index_bf = row_index_bf[col_index]
            return col_index_bf
        else:
            raise NotImplementedError()
