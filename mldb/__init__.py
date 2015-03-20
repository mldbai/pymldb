#
# __init__.py
# Mich, 2015-02-03
# Copyright (c) 2013 Datacratic. All rights reserved.
#
import requests, json, types, re
import pandas as pd


host = None

###############################################################################
# Functions used by the cell and line magics
def load_plugins(in_host):
    global host
    host = in_host
    for route, plugin_type in [("py", "python_runner"),
                               ("js", "javascript_runner")]:
                               
        resp = requests.put(host+'/v1/plugins/' + route + "runner?sync=true",
                        data=json.dumps({'type' : plugin_type}))
        if resp.status_code == 201:
            continue
        if resp.status_code == 400:
            resp = requests.get(host+'/v1/plugins/' + route + "runner")
            if resp.status_code != 200:
                raise Exception("Failed to initialize plugin " + plugin_type)
            try:
                if resp.json()['config']['type'] != plugin_type:
                    raise Exception("Failed to initialize plugin {}. "
                                    "Some other plugin was thre."
                                    .format(plugin_type))
            except Exception as e:
                print e
                raise Exception("Failed to check if {} was already loaded"
                                .format(plugin_type))

def get_usage_message():
    return ("""\
Usage:

  Line magic functions:

    %mldb help          Print this message
    
    %mldb init <url>    Initialize the plugins for the cell magics.
                        Extension comes pre-initialized with <uri> 
                        set to "http://localhost"

    %mldb py <uri>      Run a python script named "main.py" from <uri>.
                        <uri> can be one of:
                          - file://<rest of the uri>: a local directory
                          - gist://<rest of the uri>: a gist
                          - git://<rest of the uri>: a public git repo
                          
    %mldb GET <route>
    %mldb DELETE <route>
                        HTTP GET/DELETE request to <route>. <route> should
                        start with a '/'.
                        
                        
  Cell magic functions:

    %%mldb py           Run a python script in MLDB from the cell body.
    
    %%mldb query <dataset>
                        Run an SQL-like query from the cell body on 
                        <dataset> and return a pandas DataFrame.
    
    %%mldb GET <route>
                        HTTP GET request to <route>, cell body will be
                        parsed as JSON and used to create query string.
                        <route> should start with a '/'.
                        
    %%mldb PUT <route>
    %%mldb POST <route>
                        HTTP PUT/POST request to <route>, cell body will
                        be sent as JSON payload. <route> should start
                        with a '/'.
"""
    )


def add_repr_html_to_response(resp):
    def _repr_html_(self):
        result = "<strong>%s %s</strong><br />" % (self.request.method, self.request.url)
        color = "black"
        if self.status_code >= 400: color = "red" 
        if self.status_code < 300: color = "green" 
        result += "<strong style=\"color: %s;\">%d %s</strong><br /> " % (color, self.status_code, self.reason)
        if "content-type" in self.headers:
            if self.headers["content-type"] == "application/json":
                result += "<pre>%s</pre>" % json.dumps(json.loads(self.content), indent=2)
            elif self.headers["content-type"] == "text/html":
                result += self.content
        return result
    resp._repr_html_ = types.MethodType(_repr_html_, resp)
    return resp

def json_to_dataframe(resp_json):
    d = []
    has_rowName = False
    for row in resp_json:
        tmp = {}
        if "rowName" in row:
            has_rowName = True
            tmp["rowName"] = row["rowName"]
        if "columns" in row:
            for column in row["columns"]:
                tmp[column[0]] = column[1]
        d.append(tmp)
    if len(d) > 0:
        df = pd.DataFrame(d)
        if has_rowName:
            df.set_index('rowName', inplace=True)
    else:
        df = pd.DataFrame()
    return df

def run_query(ds, q):
    global host
    query = re.match(
        r"^(select (.+?))?(where (.+?))?(order by (.+?))?(group by (.+?))?$", 
        q.replace("\n", " ").strip(),
        flags=re.IGNORECASE
    )
    
    if not query.groups(): return "Unparsable query."
    params = {}
    if query.groups()[1]: params["select"]= query.groups()[1].strip()
    if query.groups()[3]: params["where"]=  query.groups()[3].strip()
    if query.groups()[5]: params["orderBy"]=query.groups()[5].strip()
    if query.groups()[7]: params["groupBy"]=query.groups()[7].strip()
        
    resp = requests.get(host+"/v1/datasets/"+ds+"/query", params=params)
    
    if resp.status_code != 200:
        return add_repr_html_to_response(resp)
    else:
        return json_to_dataframe(resp.json())


###############################################################################
# The Magic functions themselves


def mldb(line, cell=None):
    global host

    if line.strip() == "":
        print "Unknown magic.\n\n" + get_usage_message()
        return
    
    parts = line.strip().split(" ")

    # The line magics
    if cell is None:

        # Init
        if len(parts) == 2 and parts[0] == "init":
            if not parts[1].startswith("http"):
                raise Exception("URI must start with 'http'")
            load_plugins(parts[1].strip("/"))
            return

        # py or js: put a javascript or python script from an uri
        elif (len(parts) >= 2 and parts[0] in ["py", "js"]):

            type_runner = parts[0]
            payload = {"address": parts[1]}
            if len(parts) > 2:
                payload["args"] = json.loads(" ".join(parts[2:]))
            resp = requests.post(host+"/v1/plugins/" + type_runner + "runner/routes/run",
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
            print get_usage_message()
            return

        elif (len(parts) > 2 and parts[0] == "query"):

            ds = parts[1]
            return run_query(ds, " ".join(parts[2:]))

        # We have something else
        else:
            print "Unknown magic.\n\n" + get_usage_message()
            return

    # The cell magics
    else:
        # py or js: put a javascript or python script from an uri
        if (len(parts) >= 1 and parts[0] in ["py", "js"]):

            type_runner = parts[0]
            payload = {"source": cell}
            if len(parts) > 1:
                payload["args"] = json.loads(" ".join(parts[1:]))
            resp = requests.post(host+"/v1/plugins/" + type_runner + "runner/routes/run",
                             data=json.dumps(payload))
            
            return add_repr_html_to_response(resp)
        
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
            print get_usage_message()
            return

        # We have something else
        else:
            print "Unknown magic.\n\n" + get_usage_message()
            return

###############################################################################
# Load and unload the extensions
def load_ipython_extension(ipython, *args):
    ipython.register_magic_function(mldb, 'line_cell')
    load_plugins("http://localhost")

def unload_ipython_extension(ipython):
    pass
