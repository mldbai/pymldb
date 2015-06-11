# -*- coding: utf-8 -*-
# Copyright (c) 2015 Datacratic Inc.  All rights reserved.
# @Author: Alexis Tremblay
# @Email: atremblay@datacratic.com
# @Date:   2015-01-07 15:45:01
# @Last Modified by:   Alexis Tremblay
# @Last Modified time: 2015-06-02 08:32:09
# @File Name:          data.py


import pandas as pd
from pymldb.query import Query
from pymldb.index import Time, Index
import requests
import logging
import numpy as np
logging.basicConfig(level=logging.DEBUG)


class BatFrame(object):
    def __init__(self, dataset_url):
        self.dataset_url = dataset_url
        self.query = Query(dataset_url)
        self._time = Time(dataset_url)
        self._index = Index(self)

    def __getitem__(self, val):

        if isinstance(val, str):
            col = Column(val, self.dataset_url)
            col.query.mergeQuery(self.query)
            return col
        elif isinstance(val, Query):
            bf = self.copy()
            # bf.query.addSELECT('*')
            bf.query.mergeQuery(val)
            return bf
        elif isinstance(val, slice):
            start = val.start
            stop = val.stop
            # step = val.step
            bf = self.copy()
            # bf.query.addSELECT('*')
            if start is not None:
                bf.query.setOFFSET(start)
            if stop is not None:
                bf.query.setLIMIT(stop)
            return bf
        elif isinstance(val, list):
            bf = self.copy()
            for value in val:
                bf.query.addSELECT("\"{}\"".format(value))
            return bf
        elif isinstance(val, Column):
            bf = self.copy()
            bf.query.addWHERE("({})".format(val.execution_name))
            return bf

    @property
    def columns(self):
        """Returns a numpy array of the columns name"""
        return requests.get(self.dataset_url + '/columns').json()

    @property
    def rows(self):
        """Returns a numpy array of the rows name"""
        bf = self.copy()
        result = bf.query.executeQuery(format="soa")
        return result["_rowName"]

    @property
    def time(self):
        copy_time = self._time.copy()
        return copy_time.query.mergeQuery(self.Query)

    @property
    def ix(self):
        copy_index = self._index.copy()
        return copy_index

    def copy(self):
        bf = BatFrame(self.dataset_url)
        bf.query = self.query.copy()
        return bf

    def toPandas(self):
        result = self.query.executeQuery(format="aos")
        if len(result) == 0:
            return pd.DataFrame()
        return pd.DataFrame.from_records(result, index="_rowName")

    def head(self, num_rows=5):
        bf = self.copy()
        bf.query.setLIMIT(num_rows)
        return bf

    def query(self, query):
        raise NotImplementedError()

    def sort(self, value, ascending=True):
        bf = self.copy()
        if not isinstance(value, list):
            value = [value]

        if not isinstance(ascending, list):
            ascending = [ascending]*len(value)

        if len(value) != len(ascending):
            raise RuntimeError("len(value) != len(ascending)")

        for by, asc in zip(value, ascending):
            if asc:
                sort = "ASC"
            else:
                sort = "DESC"
            bf.query.addORDERBY("\"{}\" {}".format(by, sort))
        return bf

    @property
    def shape(self):
        """
        Returns (rowCount, valueCount)
        """
        bf = self.copy()
        content = requests.get(bf.dataset_url).json()
        rowCount = content['status']['rowCount']
        valueCount = content['status']['valueCount']

        return (rowCount, valueCount)

    def __repr__(self):
        bf = self.copy()
        bf.query.setLIMIT(40)
        print(bf.toPandas())
        response = requests.get(bf.dataset_url).json()
        try:
            rowCount = response['status']['rowCount']
        except:
            rowCount = None

        if rowCount is not None and rowCount > 40:
            print("{} rows".format(rowCount))
        return ""


class Column(object):

    def __init__(self, name, dataset_url):
        """
        Parameters
        ----------
        name: string
            Name of the column. No check is actually done to see if the column
            exists.
        dataset_id:
            The base url where the dataset is located.
            e.g. localhost:8888/v1/datasets/<dataset_name>
        """
        logging.debug("Instanciating Column with {}".format(name))
        self.name = "\"{}\"".format(name)
        self.execution_name = "\"{}\"".format(name)
        self.dataset_url = dataset_url
        self.query = Query(dataset_url)
        self.query.addSELECT(self.name)

    @property
    def values(self):
        result = self.query.executeQuery(format="soa")
        if len(result) > 2:
            raise RuntimeError("Only one column should be returned")
        colName = [x for x in result.keys() if x != "_rowName"][0]
        return np.array(result[colName])

    def __getitem__(self, val):

        if isinstance(val, slice):
            start = val.start
            stop = val.stop
            # step = val.step
            col = self.copy()
            if start is not None:
                col.query.setOFFSET(start)
            if stop is not None:
                col.query.setLIMIT(stop)
            return col
        elif isinstance(val, Query):
            col = self.copy()
            col.query.mergeQuery(val)
            return col
        elif isinstance(val, str):
            col = self.copy()
            col.query.addWHERE("(rowName()='{}')".format(val))
            return col


    ####################
    #  Rich comparison  #
    ####################
    def _comparison(self, value, operator):
        """
        Parameters
        ----------
        value: Column object or base type
            The value against which to compare the column. It can either be
            another column or a base type value (e.g. int)

        Returns
        -------
        self.query

        Notes
        -----
        Returning self.query will allow the next object to use this column
        ops and concatenate something else
        """
        if isinstance(value, Column):
            self.query.addWHERE("(({}){}({}))".format(
                self.execution_name,
                operator,
                value.execution_name))
        elif isinstance(value, str):
            self.query.addWHERE("(({}){}\'{}\')".format(
                self.execution_name,
                operator,
                value))
        else:
            self.query.addWHERE("(({}){}({}))".format(
                self.execution_name,
                operator,
                value))

        copy = self.copy()
        copy.query.removeSELECT("{}".format(copy.execution_name))
        return copy.query

    def __eq__(self, value):
        return self._comparison(value, '=')

    def __ne__(self, value):
        return self._comparison(value, '!=')

    def __gt__(self, value):
        return self._comparison(value, '>')

    def __ge__(self, value):
        return self._comparison(value, '>=')

    def __lt__(self, value):
        return self._comparison(value, '<')

    def __le__(self, value):
        return self._comparison(value, '<=')

    ##################################
    #  Binary arithmetic operations  #
    ##################################
    def _binary_arithemtic(self, left, binary, right):
        """
        Parameters
        ----------
        operand: Column object, integer or float
            Value on which to apply operator to this column
        binary: char
            binary arithmetic operator (-, +, *, /, ^, %)

        Returns
        -------
        self

        Notes
        -----
        Returning self will allow the next object to use this column ops and
        concatenate something else
        """
        if isinstance(right, (int, float)):
            right = right
        elif isinstance(right, Column):
            right = right.execution_name
        else:
            raise AttributeError(
                "{} can only be used ".format(binary)
                + "with integer, float or column")

        if isinstance(left, (int, float)):
            left = left
        elif isinstance(left, Column):
            left = left.execution_name
        else:
            raise AttributeError(
                "{} can only be used ".format(binary)
                + "with integer, float or column")

        copy = self.copy()
        copy.query.removeSELECT("{}".format(copy.execution_name))
        if binary == '^':  # POWER needs a different treatment
            copy.execution_name = "pow({},{})".format(left, right)
        else:
            copy.execution_name = "{}{}{}".format(left, binary, right)
        copy.query.addSELECT(copy.execution_name)

        return copy

    def __mul__(self, value):
        return self._binary_arithemtic(self, '*', value)

    def __rmul__(self, value):
        return self._binary_arithemtic(value, '*', self)

    def __div__(self, value):
        if isinstance(value, (int, float)) and value == 0:
            raise ValueError(
                "Cannot divide by zero. "
                "Do you really want to explode the planet?")
        return self._binary_arithemtic(self, '/', value)

    def __rdiv__(self, value):
        return self._binary_arithemtic(value, '/', self)

    def __truediv__(self, value):
        if isinstance(value, (int, float)) and value == 0:
            raise ValueError(
                "Cannot divide by zero. "
                "Do you really want to explode the planet?")
        return self._binary_arithemtic(self, '/', value)

    def __rtruediv__(self, value):
        return self._binary_arithemtic(value, '/', self)

    def __sub__(self, value):
        return self._binary_arithemtic(self, '-', value)

    def __rsub__(self, value):
        return self._binary_arithemtic(value, '-', self)

    def __add__(self, value):
        return self._binary_arithemtic(self, '+', value)

    def __radd__(self, value):
        return self._binary_arithemtic(value, '+', self)

    def __pow__(self, value):
        return self._binary_arithemtic(self, '^', value)

    def __rpow__(self, value):
        return self._binary_arithemtic(value, '^', self)

    def __mod__(self, value):
        return self._binary_arithemtic(self, '%', value)

    def __rmod__(self, value):
        return self._binary_arithemtic(value, '%', self)

    def __or__(self, value):
        col = self.copy()
        left = self.execution_name
        right = value

        col.query.removeSELECT(left)
        if isinstance(right, Column):
            right = value.execution_name
            col.query.removeSELECT(right)
        elif isinstance(right, Query):
            right = right.WHERE

        col.query.addWHERE('(({}) OR ({}))'.format(left, right))
        return col.query

    def __and__(self, value):
        col = self.copy()
        left = self.execution_name
        right = value

        col.query.removeSELECT(left)
        if isinstance(right, Column):
            right = value.execution_name
            col.query.removeSELECT(right)
        elif isinstance(right, Query):
            right = right.WHERE

        col.query.addWHERE('(({}) AND ({}))'.format(left, right))

        return col.query

    def __rand__(self, value):
        col = self.copy()
        left = self.execution_name
        right = value

        col.query.removeSELECT(left)
        if isinstance(right, Column):
            right = value.execution_name
            col.query.removeSELECT(right)
        elif isinstance(right, Query):
            right = right.WHERE

        col.query.addWHERE('(({}) AND ({}))'.format(right, left))

    def __ror__(self, value):
        col = self.copy()
        left = self.execution_name
        right = value

        col.query.removeSELECT(left)
        if isinstance(right, Column):
            right = value.execution_name
            col.query.removeSELECT(right)
        elif isinstance(right, Query):
            right = right.WHERE

        col.query.addWHERE('(({}) OR ({}))'.format(right, left))
        return col.query

    #################################
    #  Unary arithmetic operations  #
    #################################
    def _unary_arithmetic(self, unary):
        """
        Parameters
        ----------
        unary: char
            Unary arithmetic operator (-, +) to be applied to this column

        Returns
        -------
        self

        Notes
        -----
        Returning self will allow the next object to use this column ops and
        concatenate something else
        """
        copy = self.copy()
        copy.query.removeSELECT("{}".format(copy.execution_name))
        copy.execution_name = "{}({})".format(unary, self.execution_name)
        copy.query.addSELECT(copy.execution_name)

        return copy

    def __neg__(self):
        return self._unary_arithmetic('-')

    def __pos__(self):
        raise NotImplementedError()

    def __invert__(self):
        copy = self.copy()
        copy.execution_name = "NOT {}".format(copy.execution_name)
        return copy

    def __abs__(self):
        raise NotImplementedError()

    #############
    #  Casting  #
    #############
    def __float__(self):
        raise NotImplementedError()

    def __int__(self):
        raise NotImplementedError()

    def __long__(self):
        raise NotImplementedError()

    ###########
    #  Other  #
    ###########

    def __iter__(self):
        result = self.query.executeQuery(format="soa")
        if len(result) > 2:
            raise RuntimeError("Only one column should be returned")
        colName = [x for x in result.keys() if x != "_rowName"][0]
        values = result[colName]

        i = 0
        while i < len(values):
            yield values[i]
            i += 1

    def max(self):
        copy = self.copy()
        copy.query.removeSELECT("{}".format(copy.execution_name))
        copy.execution_name = "max({})".format(self.execution_name)
        copy.query.addSELECT(copy.execution_name)
        copy.query.addGROUPBY(1)

        result = copy.query.executeQuery(format="table")
        return result[1][1]

    def min(self):
        copy = self.copy()
        copy.query.removeSELECT("{}".format(copy.execution_name))
        copy.execution_name = "min({})".format(self.execution_name)
        copy.query.addSELECT(copy.execution_name)
        copy.query.addGROUPBY(1)

        result = copy.query.executeQuery(format="table")
        return result[1][1]

    def copy(self):
        name = self.name[1:-1]  # Removing the surrounding ''
        col = Column(name, self.dataset_url)
        col.execution_name = self.execution_name
        col.query = self.query.copy()
        return col

    def count(self):
        """Return number of non-NA/null observations in the Series"""
        raise NotImplementedError()

    def head(self, n=5):
        """Returns first n rows"""
        col = self.copy()
        col.query.setLIMIT(n)
        return col.toPandas()

    def isnull(self):
        raise NotImplementedError()

    def isin(self, values):
        raise NotImplementedError()

    def value_counts(self):
        raise NotImplementedError()

    def unique(self):
        if self.name == self.execution_name:
            url = self.dataset_url + '/columns/{}/values'.format(
                self.name[1:-1])
            logging.debug("Getting values at {}".format(url))
            return requests.get(url).json()
        else:
            result = self.query.executeQuery(format="soa")
            if len(result) > 2:
                raise RuntimeError("Only one column should be returned")
            colName = [x for x in result.keys() if x != "_rowName"][0]
            return set(result[colName])

    def sort(self, ascending=True):
        col = self.copy()

        if ascending:
            sort = "ASC"
        else:
            sort = "DESC"
        col.query.addORDERBY("{} {}".format(col.execution_name, sort))
        return col

    def toPandas(self):
        result = self.query.executeQuery(format="soa")
        if len(result) > 2:
            raise RuntimeError("Only one column should be returned")
        colName = [x for x in result.keys() if x != "_rowName"][0]
        values = result[colName]
        rowName = result["_rowName"]
        if len(values) > 0:
            s = pd.Series(values, index=rowName)
        else:
            s = pd.Series()
        return s

    def __repr__(self):
        col = self.copy()
        col.query.setLIMIT(40)
        print(col.toPandas())
        response = requests.get(col.dataset_url).json()
        try:
            rowCount = response['status']['rowCount']
        except:
            rowCount = None

        if rowCount is not None and rowCount > 40:
            print("{} rows".format(rowCount))
        return ""
