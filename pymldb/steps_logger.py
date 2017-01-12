#
# steps_logger.py
# Mich, 2016-10-27
# Copyright (c) 2016 Datacratic. All rights reserved.
#

#from IPython.display import display, HTML
from tqdm import tqdm, tqdm_notebook

class NotebookStepsLogger(object):

    def __init__(self):
        self.steps = {}

    def __del__(self):
        for _, step in self.steps.iteritems():
            if not step['done']:
                step['pbar'].sp(bar_style='danger')
                step['pbar'].close()

    def init_progress(self, steps_update):
        for step_update in steps_update:
            self.steps[step_update['name']] = {
                'pbar' : tqdm_notebook(desc=step_update['name'],
                                       unit='items',
                                       unit_scale=True),
                'done' : False
            }

    def log_progress_steps(self, steps_update):
        if len(self.steps) == 0:
            self.init_progress(steps_update)

        for step_update in steps_update:
            step = self.steps[step_update['name']]
            if 'ended' in step_update:
                if step['done']:
                    # already done, skip
                    continue


                step['done'] = True
                step['pbar'].n = step_update['value'];
                step['pbar'].update(0)
                step['pbar'].close()
            elif 'started' in step_update:
                step['pbar'].n = step_update['value'];
                step['pbar'].update(0)

    def clean_finish(self):
        for _, step in self.steps.iteritems():
            if not step['done']:
                step['pbar'].close()
        self.steps = {}

class StepsLogger(object):

    def __init__(self):
        self.steps = {}

    def init_progress(self, steps_update):
        for step_update in steps_update:
            self.steps[step_update['name']] = {
                'pbar' : None,
                'done' : False
            }

    def log_progress_steps(self, steps_update):
        if len(self.steps) == 0:
            self.init_progress(steps_update)

        for step_update in steps_update:
            step = self.steps[step_update['name']]
            if 'ended' in step_update:
                if step['done']:
                    # already done, skio
                    continue


                step['done'] = True
                step['pbar'].n = step_update['value'];
                step['pbar'].update(0) #step_update['value'] - step['pbar'])
                step['pbar'].close()
            elif 'started' in step_update:
                if step['pbar'] is None:
                    step['pbar'] = tqdm(desc=step_update['name'],
                                        unit='items',
                                        unit_scale=True)
                step['pbar'].n = step_update['value'];
                step['pbar'].update(0)
