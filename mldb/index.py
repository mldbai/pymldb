# -*- coding: utf-8 -*-
# Copyright (c) 2015 Datacratic Inc.  All rights reserved.
# @Author:             Alexis Tremblay
# @Email:              atremblay@datacratic.com
# @Date:               2015-03-19 10:28:23
# @Last Modified by:   Alexis Tremblay
# @Last Modified time: 2015-04-09 14:44:44
# @File Name:          index.py

from mldb.query import Query


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
        print("Index.__getitem__")
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
        else:
            raise NotImplementedError()
