import time
from rel import timeout

class TimeLoop:
    def __init__(self, interval_seconds: float, function, *args, **kwargs):
        self.interval = interval_seconds
        self.function = function
        self.next_time = time.time() + self.interval
        self.timer = timeout(self.next_time - time.time(), self.update)
    
    def update(self):
        self.next_time = self.next_time + self.interval
        self.timer = timeout(max(0, self.next_time - time.time()), self.update)
        self.function()
    
    def stop(self):
        self.timer.cancel()
