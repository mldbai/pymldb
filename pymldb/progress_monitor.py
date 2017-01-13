#sad
# progress_monitor.py
# Mich, 2017-01-12
# Copyright (c) 2017 Datacratic. All rights reserved.
#
# Tracks the progression of a procedure as long as it is running and updates
# progress output via a StepLogger. When in notebook mode, also displays a
# cancel button. The button # will fail if the notebook is not under the same
# host:port than MLDB because of cross origin policies.
#
from __future__ import absolute_import, division, print_function
import threading
import requests
from .steps_logger import getStepsLogger

class ProgressMonitor(object):

    def __init__(self, conn, refresh_rate_sec, proc_id, run_id=None,
                 notebook=True):
        self.conn = conn
        self.refresh_rate_sec = refresh_rate_sec
        self.proc_id = proc_id
        self.run_id = run_id
        self.event = threading.Event()
        self.notebook = notebook
        if self.notebook:
            from IPython.display import display, HTML
            self.display_html = lambda x: display(HTML(x))

    def monitor_progress(self):
        # wrap everything in a try/except because exceptions are not passed to
        # mldb.log by themselves.
        proc_id = self.proc_id
        run_id = self.run_id
        conn = self.conn
        refresh_rate_sec = 0.5
        run_id_flat = None
        try:
            # find run id
            sl = getStepsLogger(self.notebook)
            while not self.event.wait(refresh_rate_sec):
                if run_id is None:
                    res = conn.get('/v1/procedures/{}/runs'.format(proc_id)).json()
                    if res:
                        run_id = res[0]
                    else:
                        continue
                refresh_rate_sec = self.refresh_rate_sec
                if run_id_flat is None:
                    run_id_flat = run_id
                    for c in '-.:':
                        run_id_flat = run_id_flat.replace(c, '_')
                    if conn.uri == 'localhost':
                        host = ''
                    else:
                        host = conn.uri
                    if self.notebook:
                        self.display_html("""
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
                        """.format(run_id=run_id, run_id_flat=run_id_flat, proc_id=proc_id, host=host))
                res = requests.get(conn.uri + '/v1/procedures/{}/runs/{}'.format(proc_id, run_id)).json()
                if res['state'] == 'executing':
                    sl.log_progress_steps(res['progress']['steps'])
                else:
                    break
            if run_id is not None:
                if self.notebook:
                    self.display_html("""
                        <script type="text/javascript" class="removeMe">
                            $(function() {{
                                var outputArea = $(".removeMe").parents(".output_area:first");
                                outputArea.prevAll().remove();
                                outputArea.next().remove();
                                outputArea.remove();
                            }})
                        </script>
                    """.format(run_id_flat=run_id_flat))
                res = requests.get(conn.uri + '/v1/procedures/{}/runs/{}'.format(proc_id, run_id)).json()
                if res['state'] == 'finished':
                    sl.clean_finish()

        except Exception as e:
            print(str(e))
            import traceback
            print(traceback.format_exc())
