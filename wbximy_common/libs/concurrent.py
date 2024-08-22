# encoding=utf8

import logging
from concurrent.futures import ThreadPoolExecutor
from threading import BoundedSemaphore

logger = logging.getLogger(__name__)


# https://gist.github.com/frankcleary/f97fe244ef54cd75278e521ea52a697a
class BoundedExecutor(ThreadPoolExecutor):
    """BoundedExecutor behaves as a ThreadPoolExecutor which will block on
    calls to submit() once the limit given as "bound" (max_workers*cache_factor) work items are queued for
    execution.
    """
    def __init__(self, max_workers: int = 1, cache_factor: int = 8, **kwargs):
        self.semaphore = BoundedSemaphore(max_workers * cache_factor)
        super().__init__(max_workers=max_workers, **kwargs)

    # See concurrent.futures.Executor#submit
    def submit(self, fn, *args, **kwargs):
        self.semaphore.acquire()
        try:
            future = super().submit(fn, *args, **kwargs)
        except Exception as e:
            self.semaphore.release()
            raise e
        else:
            future.add_done_callback(lambda x: self.semaphore.release())
            return future
