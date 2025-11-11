"""
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/
"""


import threading

'''
The Controller class provides centralized thread control for the video streaming system.
It manages pause, resume, and stop signals for the display and writer threads (and optionally the producer), 
using threading.Event objects for clean, non-blocking synchronization.
'''

class Controller:
    def __init__(self):

        self.modules = {
            "stream" : threading.Event(),  # pause/resume the pull of video streams
            "storage": threading.Event(),  # pause/resume the display
            "display": threading.Event(),  # pause/resume the pull of video streams
        }

        self.resume_all()       # start un-paused

        self.exit_flag = False

        self.ai_models = False # Changed to True if processing is with AI models

    def pause_event(self, module_name): self.modules[module_name].clear()    # marks it as not set (wait() will block).
    def resume_event(self, module_name): self.modules[module_name].set()     # marks the event as set (any waiting threads unblock)
    def get_state(self, module_name): return self.modules[module_name].is_set()        # returns a boolean (True / False) reflecting the current state.
    def thread_stream_wait(self, module_name): self.modules[module_name].wait()        # event.wait() → blocks until the event is set.

    def resume_all(self):
        # Exit all threads from pause mode
        for event in self.modules.values():
            event.set()  # start un-paused

    def set_exit(self): self.exit_flag = True           # When exit is called
    def is_exit(self): return self.exit_flag

    def reset_exit (self): self.exit_flag = False

    def use_ai_models(self, flag):  self.ai_models = flag
    def with_ai_models(self): return self.ai_models      # Return True if process with AI Models