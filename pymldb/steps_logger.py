#
# steps_logger.py
# Mich, 2016-10-27
# Copyright (c) 2016 Datacratic. All rights reserved.
#

from datetime import datetime
from dateutil.tz import tzutc
from dateutil.parser import parse as parse_date
from IPython.display import display, HTML

class StepsLogger(object):

    def __init__(self):
        self.done_steps = set()

    def log_progress_steps(self, progress_steps):
        now = datetime.now(tzutc())
        for step in progress_steps:
            if 'ended' in step:
                if step['name'] in self.done_steps:
                    continue
                self.done_steps.add(step['name'])
                ran_in = parse_date(step['ended']) - parse_date(step['started'])
                display(HTML("{} completed in {} seconds - {} {}"
                             .format(step['name'], ran_in.total_seconds(),
                                     step['type'], step['value'])))
            elif 'started' in step:
                running_since = now - parse_date(step['started'])
                display(HTML('<span class="partial">{} runing since {} seconds - {} {}</span>'
                             .format(step['name'], running_since.total_seconds(),
                                     step['type'], step['value'])))

