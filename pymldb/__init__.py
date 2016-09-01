#
# pymldb
# Nicolas Kructhen, 2015-05-28
# Mich, 2016-01-26
# Copyright (c) 2013 Datacratic. All rights reserved.
#

import pandas as pd
import requests
import json
from pymldb.util import add_repr_html_to_response


def decorate_response(fn):
    def inner(*args, **kwargs):
        result = add_repr_html_to_response(fn(*args, **kwargs))
        if result.status_code < 200 or result.status_code >= 400:
            raise ResourceError(result)
        return result
    return inner


class ResourceError(Exception):
    def __init__(self, r):
        try:
            message = json.dumps(r.json(), indent=2)
        except:
            message = r.content
        super(ResourceError, self).__init__(
            "'%d %s' response to '%s %s'\n\n%s" %
            (r.status_code, r.reason, r.request.method, r.request.url, message)
        )
        self.result = r


class Connection(object):

    def __init__(self, host="http://localhost"):
        if not host.startswith("http"):
            raise Exception("URIs must start with 'http'")
        if host[-1] == '/':
            host = host[:-1]
        self.uri = host

    @decorate_response
    def get(self, url, data=None, **kwargs):
        params = {}
        for k, v in kwargs.iteritems():
            if type(v) in [dict, list]:
                v = json.dumps(v)
            params[str(k)] = v
        return requests.get(self.uri + url, params=params, json=data)

    @decorate_response
    def put(self, url, payload=None):
        if payload is None:
            payload = {}
        return requests.put(self.uri + url, json=payload)

    @decorate_response
    def post(self, url, payload=None):
        if payload is None:
            payload = {}
        return requests.post(self.uri + url, json=payload)

    @decorate_response
    def delete(self, url):
        return requests.delete(self.uri + url)

    def query(self, sql, **kwargs):
        """
        Shortcut for GET /v1/query, except with argument format='dataframe'
        (the default), in which case it will simply wrap the result of the GET
        query to /v1/query (with format='table') in a `pandas.DataFrame`.
        """
        if 'format' not in kwargs or kwargs['format'] == 'dataframe':
            resp = self.get('/v1/query', data={'q': sql, 'format': 'table'}).json()
            if len(resp) == 0:
                return pd.DataFrame()
            else:
                return pd.DataFrame.from_records(resp[1:], columns=resp[0],
                                                index="_rowName")
        kwargs['q'] = sql
        return self.get('/v1/query', **kwargs).json()
