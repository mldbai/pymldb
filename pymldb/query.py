# -*- coding: utf-8 -*-
# Copyright (c) 2015 Datacratic Inc.  All rights reserved.
# @Author:             Alexis Tremblay
# @Email:              atremblay@datacratic.com
# @Date:               2015-03-06 14:53:37
# @Last Modified by:   Alexis Tremblay
# @Last Modified time: 2015-04-09 16:54:58
# @File Name:          query.py


import json
import requests
import traceback
import copy
from collections import Counter
import logging
logging.basicConfig(level=logging.DEBUG)


class Query(object):
    """docstring for Query"""
    def __init__(self, dataset_url):
        self.dataset_url = dataset_url
        self.SELECT = Counter()
        self.WHERE = None # list()
        self.GROUPBY = list()
        self.OFFSET = None
        self.LIMIT = None
        self.ORDERBY = list()

    def addSELECT(self, obj):
        logging.debug("Adding SELECT {}".format(obj))
        self.SELECT[obj] += 1
        logging.debug(self.SELECT)

    def removeSELECT(self, obj):
        logging.debug("Removing SELECT {}".format(obj))
        if obj not in self.SELECT:
            return

        self.SELECT[obj] -= 1
        if self.SELECT[obj] == 0:
            del self.SELECT[obj]
        logging.debug(self.SELECT)

    def mergeSELECT(self, query):
        self.SELECT = self.SELECT + query.SELECT

    def addWHERE(self, where, boolean=None):
        if where is None:
            return

        if self.WHERE is None:
            self.WHERE = where
        else:
            # if boolean is None:
            #     raise RuntimeError("Must provide boolean instruction to WHERE")
            if boolean != "OR" and boolean != "AND":
                raise RuntimeError("Boolean instruction must OR or AND")
            self.WHERE = "({} {} {})".format(self.WHERE, boolean, where)

        # self.WHERE.append(where)

    def mergeWHERE(self, query, how):
        self.addWHERE(query.WHERE, how)
        # self.WHERE.extend(query.WHERE)

    def addGROUPBY(self, value):
        self.GROUPBY.append(str(value))

    def mergeGROUPBY(self, query):
        self.GROUPBY.extend(query.GROUPBY)

    def setOFFSET(self, value):
        # Basically the start of slicing. This can normally be a negative
        # number in python. For now (and probably forever), not supported.
        # i.e. my_list[-10:] is not supported
        if type(value) != int:
            raise RuntimeError("Can only slice with integer")
        if value < 0:
            raise RuntimeError("Slicing with negative index is not allowed")
        if self.OFFSET is None:
            self.OFFSET = value
        if self.OFFSET < value:
            self.OFFSET = value

    def setLIMIT(self, value):
        # Basically the stop of slicing. This can normally be a negative
        # number in python. For now (and probably forever), not supported.
        # i.e. my_list[:-1] is not supported
        if type(value) != int:
            raise RuntimeError("Can only slice with integer")
        if value < 0:
            raise RuntimeError("Slicing with negative index is not allowed")
        if self.LIMIT is None:
            self.LIMIT = value
        if self.LIMIT > value:
            self.LIMIT = value

    def addORDERBY(self, value):
        self.ORDERBY.append(value)

    def mergeORDERBY(self, query):
        self.ORDERBY.extend(query.ORDERBY)

    def mergeQuery(self, query, how=None):
        self.mergeSELECT(query)
        self.mergeWHERE(query, how)
        self.mergeGROUPBY(query)
        self.mergeORDERBY(query)

        if self.OFFSET is not None and query.OFFSET is not None:
            raise RuntimeError("Multiple slicing asked")

        if self.OFFSET is None:
            self.OFFSET = query.OFFSET

        if self.LIMIT is not None and query.LIMIT is not None:
            raise RuntimeError("Multiple slicing asked")

        if self.LIMIT is None:
            self.LIMIT = query.LIMIT

    def buildQuery(self):
        data = {}
        # print("Building query")
        # print(self.SELECT, len(self.SELECT))
        if len(self.SELECT) == 0:
            # print("Replacing SELECT with *")
            data["select"] = '*'
        else:
            data["select"] = ",".join(self.SELECT.keys())

        if self.WHERE is not None:
            # data["where"] = " ".join(self.WHERE)
            data["where"] = self.WHERE
        if len(self.GROUPBY) > 0:
            data["groupBy"] = ",".join(self.GROUPBY)
        if self.OFFSET is not None:
            data["offset"] = self.OFFSET
        if self.LIMIT is not None:
            data["limit"] = self.LIMIT
        if len(self.ORDERBY) > 0:
            data["orderBy"] = ",".join(self.ORDERBY)

        return data

    def executeQuery(self, format):

        query = self.buildQuery()
        query["format"] = format
        logging.debug("REST params\n{}".format(json.dumps(query)))

        select_url = self.dataset_url + "/query"

        try:
            # logging.info(select_url)
            response = requests.get(select_url, params=query)
            logging.info("URL poked {}".format(response.url))
        except requests.HTTPError as e:
            logging.error("Code: {}\nReason: {}".format(
                e.status_code, e.reason))
            logging.error("Content: {}".format(response.content))
            logging.error(traceback.format_exc())

        if response.status_code != 200:
            logging.error("Code: {}\nReason: {}".format(
                response.status_code, response.reason))
            logging.error("Content: {}".format(response.content))
            logging.error(traceback.format_exc())

        try:
            return response.json()
        except:
            return {}

    def __or__(self, value):
        if isinstance(value, Query):
            query = self.copy()
            # self.addWHERE('OR')
            query.mergeQuery(value, "OR")
        return query

    def __and__(self, value):
        if isinstance(value, Query):
            query = self.copy()
            # self.addWHERE('AND')
            query.mergeQuery(value, "AND")
        return query

    def __rand__(self, value):
        raise NotImplementedError()

    def __ror__(self, value):
        raise NotImplementedError()

    def copy(self):
        query = Query(self.dataset_url)
        query.SELECT = copy.deepcopy(self.SELECT)
        query.WHERE = copy.deepcopy(self.WHERE)
        query.ORDERBY = copy.deepcopy(self.ORDERBY)
        query.GROUPBY = copy.deepcopy(self.GROUPBY)
        query.OFFSET = copy.deepcopy(self.OFFSET)
        query.LIMIT = copy.deepcopy(self.LIMIT)
        return query

    def __repr__(self):
        return json.dumps(self.buildQuery(), indent=4)

    def __str__(self):
        return json.dumps(self.buildQuery(), indent=4)

