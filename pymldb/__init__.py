#
# pymldb
# Nicolas Kructhen, 2015-05-28
# Mich, 2016-01-26
# Copyright (c) 2013 Datacratic. All rights reserved.
#

from version import __version__

#import pandas as pd
import requests
import json
from pymldb.util import add_repr_html_to_response
import threading
from steps_logger import StepsLogger
from IPython.display import display, HTML


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

    def post_run_and_track_procedure(self, payload, refresh_rate_sec=10):
        if 'params' not in payload:
            payload['params'] = {}
        payload['params']['runOnCreation'] = False

        res = requests.post(self.uri + '/v1/procedures', json=payload).json()
        proc_id = res['id']
        event = threading.Event()

        def monitor_progress():
            # wrap everything in a try/except because exceptions are not passed to
            # mldb.log by themselves.
            try:
                # find run id
                run_id = None
                sl = StepsLogger()
                while not event.wait(refresh_rate_sec):
                    if run_id is None:
                        res = self.get('/v1/procedures/{}/runs'.format(proc_id)).json()
                        if res:
                            run_id = res[0]
                        else:
                            continue
                        run_id_flat = run_id
                        for c in '-.:':
                            run_id_flat = run_id_flat.replace(c, '_')
                        if self.uri == 'localhost':
                            host = ''
                            url.format(host='', proc_id=proc_id, run_id=run_id)
                        else:
                            host = self.uri
                        display(HTML("""
                            <script type="text/javascript">
                                function cancel_{run_id_flat}(btn) {{
                                    $(btn).attr("disabled", true).html("Cancelling...");
                                    $.ajax({{
                                        url: "{host}/v1/procedures/{proc_id}/runs/{run_id}/state",
                                        type: 'PUT',
                                        data: JSON.stringify({{"state" : "cancelled"}}),
                                        success: () => {{ $(btn).html("Cancelled"); }},
                                        error: (xhr) => {{ console.error(xhr);
                                                           console.warn("If this is a Cross-Origin Request, this is a normal error. Otherwise you may report it.");
                                                           $(btn).html("Cancellation failed - See JavaScript console");
                                                        }}
                                    }});
                                }}
                            </script>
                            <button id="{run_id_flat}" onclick="cancel_{run_id_flat}(this);">Cancel</button>
                        """.format(run_id=run_id, run_id_flat=run_id_flat, proc_id=proc_id, host=host)))
                    res = requests.get(self.uri + '/v1/procedures/{}/runs/{}'.format(proc_id, run_id)).json()
                    if res['state'] == 'executing':
                        display(HTML("""
                            <script type="text/javascript" class="partial">
                                $(".partial").parent().remove();
                            </script>
                        """))
                        sl.log_progress_steps(res['progress']['steps'])
                    else:
                        break
                if run_id is not None:
                    display(HTML("""
                        <script type="text/javascript">
                            $(function() {{
                                $("#{run_id_flat}").remove();
                            }})
                        </script>
                    """.format(run_id_flat=run_id_flat)))


            except Exception as e:
                print str(e)
                import traceback
                print traceback.format_exc()

        t = threading.Thread(target=monitor_progress)
        t.start()

        try:
            return requests.post(self.uri + '/v1/procedures/{}/runs'.format(proc_id), json={}).json()
        except Exception as e:
            print e
        finally:
            event.set()
            t.join()

