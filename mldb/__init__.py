#
# __init__.py
# Mich, 2015-02-03
# Copyright (c) 2013 Datacratic. All rights reserved.
#
import requests, json, types
import pandas as pd
from pygments import highlight
from pygments.lexers import JsonLexer
from pygments.formatters import HtmlFormatter
from pygments.style import Style
from pygments.token import Keyword, Name, String, Number

class PygmentsStyle(Style):
    default_style = ""
    styles = {
        Name: 'bold #333', String: '#00d', Number: '#00d', Keyword: '#00d'
    }

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

    %mldb py <uri> <json args>
                        Run a python script named "main.py" from <uri>
                        and pass in <json args> as arguments.
                        <uri> can be one of:
                          - file://<rest of the uri>: a local directory
                          - gist://<rest of the uri>: a gist
                          - git://<rest of the uri>: a public git repo

    %mldb pyplugin <name> <uri>
                        Load a python plugin called <name> from <uri> 
                        by executing its main.py. Any pre-existing plugin
                        called <name> will be deleted first.
                        <uri> can be one of:
                          - file://<rest of the uri>: a local directory
                          - gist://<rest of the uri>: a gist
                          - git://<rest of the uri>: a public git repo
                          
    %mldb GET <route>
    %mldb DELETE <route>
                        HTTP GET/DELETE request to <route>. <route> should
                        start with a '/'.
                        
                        
  Cell magic functions:

    %%mldb py <json args>
    <python code>
                        Run a python script in MLDB from the cell body.
    
    %%mldb query <dataset>
    <sql>
                        Run an SQL-like query from the cell body on 
                        <dataset> and return a pandas DataFrame.
    
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
        print "Unknown magic..."
        print ""
    print msg


def add_repr_html_to_response(resp):
    def _repr_html_(self):
        result = "<strong>%s %s</strong><br />" % (self.request.method, self.request.url)
        color = "black"
        if self.status_code >= 400: color = "red" 
        if self.status_code < 300: color = "green" 
        result += "<strong style=\"color: %s;\">%d %s</strong><br /> " % (color, self.status_code, self.reason)
        if "content-type" in self.headers:
            if self.headers["content-type"] == "application/json":
                result += highlight(
                    json.dumps(self.json(), indent=2), 
                    JsonLexer(), 
                    HtmlFormatter(noclasses = True, nobackground =True, style=PygmentsStyle)
                    )
            elif self.headers["content-type"] == "text/html":
                result += self.content
        return result
    resp._repr_html_ = types.MethodType(_repr_html_, resp)
    return resp

def handle_script_output(resp):
    result = resp.json()
    if "out" in result:
        for o in result["out"]:
            print o[2]
    if "exception" in result:
        for e in result["exception"]["stack"]:
            print e 
    if "return" in result:
        return result["return"]

def json_to_dataframe(resp_json):
    d = []
    for row in resp_json:
        tmp = {}
        tmp["rowName"] = row["rowName"]
        if "columns" in row:
            for column in row["columns"]:
                tmp[column[0]] = column[1]
        d.append(tmp)
    if len(d) > 0:
        df = pd.DataFrame(d)
        df.set_index('rowName', inplace=True)
    else:
        df = pd.DataFrame()
    return df

def run_query(ds, q):
    global host

    resp = requests.get(host+"/v1/datasets/"+ds+"/rawquery", params={"q": q})
    
    if resp.status_code != 200:
        return add_repr_html_to_response(resp)
    else:
        return json_to_dataframe(resp.json())


###############################################################################
# The Magic functions themselves


def mldb(line, cell=None):
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

        # help
        elif len(parts) == 1 and parts[0] == "help":
            return print_usage_message()

        elif (len(parts) > 2 and parts[0] == "query"):

            ds = parts[1]
            return run_query(ds, " ".join(parts[2:]))

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
        
        if (len(parts) == 2 and parts[0] == "query"):

            _, ds = parts
            return run_query(ds, cell)

        # perform
        elif (len(parts) == 2 and parts[0] in ["GET", "PUT", "POST"]):

            verb, uri = parts
            payload = json.loads(cell)
            if verb == "GET":
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

###############################################################################
# Load and unload the extensions
def load_ipython_extension(ipython, *args):
    ipython.register_magic_function(mldb, 'line_cell')

def unload_ipython_extension(ipython):
    pass
