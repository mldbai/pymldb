# -*- coding: utf-8 -*-
# Copyright (c) 2015 Datacratic Inc.  All rights reserved.
# @Author:             Alexis Tremblay
# @Email:              atremblay@datacratic.com
# @Date:               2015-03-19 10:28:23
# @Last Modified by:   Alexis Tremblay
# @Last Modified time: 2015-03-19 10:34:00
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
