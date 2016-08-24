#
# magic.py
# Copyright (c) 2013 Datacratic. All rights reserved.
#

import requests, json
from pymldb.util import add_repr_html_to_response
import pandas as pd

host = "http://localhost"

###############################################################################
# Functions used by the cell and line magics

def print_usage_message(unknown = False):
    msg = """\
Usage:

  Line magic functions:

    %mldb help          
                        Print this message
    
    %mldb init <url>    
                        Initialize the plugins for the cell magics.
                        Extension comes pre-initialized with <uri> 
                        set to "http://localhost"
    
    %mldb doc <kind>/<type>    
                        Shows documentation in an iframe. <kind> can
                        be one of "datasets", "blocks", "pipelines" or
                        "plugins" and <type> can be one of the installed
                        types, e.g. pipelines/classifier. NB this will 
                        only work with an MLDB-hosted Notebook for now.

    %mldb query <sql>
                        Run an SQL-like query and return a pandas 
                        DataFrame. Dataset selection is done via the 
                        FROM clause.

    %mldb loadcsv <dataset> <url>
                        Create a dataset with id <dataset> from a CSV
                        hosted at the HTTP url <url>.
                        
    %mldb py <uri> <json args>
                        Run a python script named "main.py" from <uri>
                        and pass in <json args> as arguments.
                        <uri> can be one of:
                          - file://<rest of the uri>: a local directory
                          - gist://<rest of the uri>: a gist
                          - git://<rest of the uri>: a public git repo
                          - http(s)://<rest of the uri>: a file on the web

    %mldb pyplugin <name> <uri>
                        Load a python plugin called <name> from <uri> 
                        by executing its main.py. Any pre-existing plugin
                        called <name> will be deleted first.
                        <uri> can be one of:
                          - file://<rest of the uri>: a local directory
                          - gist://<rest of the uri>: a gist
                          - git://<rest of the uri>: a public git repo
                          - http(s)://<rest of the uri>: a file on the web
                          
    %mldb GET <route>
    %mldb DELETE <route>
                        HTTP GET/DELETE request to <route>. <route> should
                        start with a '/'.
                        
    %mldb GET <route> <json query params>       
                        HTTP GET request to <route>, JSON will be used to       
                        create query string. <route> should start with a '/'.       
                        
    %mldb PUT <route> <json>
    %mldb POST <route> <json>
                        HTTP PUT/POST request to <route>, <json> will
                        be sent as JSON payload. <route> should start
                        with a '/'.
       
                        
  Cell magic functions:

    %%mldb py <json args>
    <python code>
                        Run a python script in MLDB from the cell body.
    
    %%mldb query
    <sql>
                        Run an SQL-like query from the cell body and return
                        a pandas DataFrame. Dataset selection is done via
                        the FROM clause.
    
    %mldb loadcsv <dataset>
    <csv>
                        Create a dataset with id <dataset> from a CSV
                        in the cell body.
                        
    %%mldb GET <route>
    <json query params>
                        HTTP GET request to <route>, cell body will be
                        parsed as JSON and used to create query string.
                        <route> should start with a '/'.
                        
    %%mldb PUT <route>
    <json>
    %%mldb POST <route>
    <json>
                        HTTP PUT/POST request to <route>, cell body will
                        be sent as JSON payload. <route> should start
                        with a '/'.
"""
    if unknown:
        print("Unknown magic...")
        print("")
    print(msg)



def load_csv(dataset, csv_input):

    payload = {"args": [dataset, csv_input]}
    payload["source"] = """
import csv, urllib, StringIO
if mldb.script.args[1].startswith("http"):
    reader = csv.DictReader(open(urllib.urlretrieve(mldb.script.args[1])[0]))
else:
    reader = csv.DictReader(StringIO.StringIO(mldb.script.args[1]))

dataset = mldb.create_dataset(dict(id=mldb.script.args[0], type="beh.mutable"))
for i, row in enumerate(reader):
    values = []
    row_name = i
    for col in row:
        if col == "":
            row_name = row[col]
        else:
            values.append([col, row[col], 0])
    dataset.record_row(row_name, values)
dataset.commit()
print "Success!"
    """
    resp = requests.post(host+"/v1/types/plugins/python/routes/run",
                     data=json.dumps(payload))
    return handle_script_output(resp)

def handle_script_output(resp):
    if resp.status_code != 200:
        return add_repr_html_to_response(resp)
    
    result = resp.json()
    if "out" in result:
        for o in result["out"]:
            print(o[2])
    if "exception" in result:
        for e in result["exception"]["stack"]:
            print(e) 
    if "return" in result:
        return result["return"]

def run_query(q):
    global host

    resp = requests.get(host+"/v1/query", 
        data={"q": q, "format": "aos"})

    if resp.status_code != 200:
        return add_repr_html_to_response(resp)

    resp_json = resp.json()
    if len(resp_json) == 0: 
        return pd.DataFrame()
    else:
        return pd.DataFrame.from_records(resp_json, index="_rowName")


###############################################################################
# The Magic functions themselves


def dispatcher(line, cell=None):
    global host

    if line.strip() == "":
        return print_usage_message(True)
        
    
    parts = line.strip().split(" ")

    # The line magics
    if cell is None:

        # Init
        if len(parts) == 2 and parts[0] == "init":
            if not parts[1].startswith("http"):
                raise Exception("URI must start with 'http'")
            host = parts[1].strip("/")
            print("%mldb magic initialized with host as " + host)
            return

        # py or js: put a javascript or python script from an uri
        elif (len(parts) >= 2 and parts[0] in ["py", "js"]):

            type_name = "python"
            if parts[0] == "js":
                type_name = "javascript"
            payload = {"address": parts[1]}
            if len(parts) > 2:
                payload["args"] = json.loads(" ".join(parts[2:]))
            resp = requests.post(host+"/v1/types/plugins/" + type_name + "/routes/run",
                             data=json.dumps(payload))
            return handle_script_output(resp)
        
        # doc
        elif (len(parts) == 2 and parts[0] == "doc"):
            from IPython.display import IFrame
            return IFrame(src="/v1/types/"+parts[1]+"/doc", width=900, height=500)
        
        # pyplugin
        elif (len(parts) == 3 and parts[0] == "pyplugin"):
    
            name = parts[1]
            payload = {"type":"python", "params": {"address": parts[2]}}
            requests.delete(host+"/v1/plugins/" + name)
            resp = requests.put(host+"/v1/plugins/" + name,
                             data=json.dumps(payload))
            return add_repr_html_to_response(resp)

        # perform
        elif (len(parts) == 2 and parts[0] in ["GET", "DELETE"]):

            verb, uri = parts
            if verb == "GET":
                resp = requests.get(host+uri)
            elif verb == "DELETE":
                resp = requests.delete(host+uri)
                
            return add_repr_html_to_response(resp)

        # perform 
        elif (len(parts) > 2 and parts[0] in ["GET", "PUT", "POST"]):

            verb = parts[0]
            uri = parts[1]
            payload = json.loads(" ".join(parts[2:]))
            if verb == "GET":       
                for k in payload:       
                    if isinstance(payload[k], dict):        
                        payload[k] = json.dumps(payload[k])
                resp = requests.get(host+uri, params=payload)
            elif verb == "PUT":
                resp = requests.put(host+uri, data=json.dumps(payload))
            elif verb == "POST":
                resp = requests.post(host+uri, data=json.dumps(payload))
                
            return add_repr_html_to_response(resp)

        # help
        elif len(parts) == 1 and parts[0] == "help":
            return print_usage_message()

        elif (len(parts) > 1 and parts[0] == "query"):
            return run_query(" ".join(parts[1:]))
        
        elif (len(parts) == 3 and parts[0] == "loadcsv"):
            return load_csv(parts[1], parts[2])

        # We have something else
        else:
            return print_usage_message(True)

    # The cell magics
    else:
        # py or js: put a javascript or python script from an uri
        if (len(parts) >= 1 and parts[0] in ["py", "js"]):

            type_name = "python"
            if parts[0] == "js":
                type_name = "javascript"
            payload = {"source": cell}
            if len(parts) > 1:
                payload["args"] = json.loads(" ".join(parts[1:]))
            resp = requests.post(host+"/v1/types/plugins/" + type_name + "/routes/run",
                             data=json.dumps(payload))
            
            return handle_script_output(resp)
        
        if (len(parts) == 1 and parts[0] == "query"):
            return run_query(cell.replace("\n", " ").strip())

        if (len(parts) == 2 and parts[0] == "loadcsv"):
            return load_csv(parts[1], cell)

        # perform
        elif (len(parts) == 2 and parts[0] in ["GET", "PUT", "POST"]):

            verb, uri = parts
            payload = json.loads(cell)
            if verb == "GET":
                for k in payload:
                    if isinstance(payload[k], dict):
                        payload[k] = json.dumps(payload[k])
                        
                resp = requests.get(host+uri, params=payload)
            elif verb == "PUT":
                resp = requests.put(host+uri, data=json.dumps(payload))
            elif verb == "POST":
                resp = requests.post(host+uri, data=json.dumps(payload))
                
            return add_repr_html_to_response(resp)
        # help
        elif len(parts) == 1 and parts[0] == "help":
            return print_usage_message()
            

        # We have something else
        else:
            return print_usage_message(True)

