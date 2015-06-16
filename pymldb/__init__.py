#
# pymldb
# Copyright (c) 2013 Datacratic. All rights reserved.
#

from pymldb import resource, data
import pandas as pd


class Connection(object):
    
    def __init__(self, host="http://localhost"):
        if not host.startswith("http"): 
            raise Exception("URIs must start with 'http'")
        self.host = host.strip("/")
        self.v1 = resource.Resource(self.host).v1
        
    def query(self, sql):
        resp = self.v1.query.get(params=dict(q=sql, format="aos"))
        if resp.status_code != 200: return resp
        resp_json = resp.json()
        if len(resp_json) == 0: 
            return pd.DataFrame()
        else:
            return pd.DataFrame.from_records(resp_json, index="_rowName")
    
    def batframe(self, dataset_id):
        return data.BatFrame(self.v1.datasets(dataset_id).uri)


# IPython Magic system

def load_ipython_extension(ipython, *args):
    from pymldb.magic import dispatcher
    dispatcher("init http://localhost")
    ipython.register_magic_function(dispatcher, 'line_cell', magic_name="mldb")

def unload_ipython_extension(ipython):
    pass