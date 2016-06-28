import logging
from multiprocessing.pool import ThreadPool


def batch(iterable, batch_size=1):
    """
    Example usage:

    >>> list(batch([1, 2, 3, 4, 5, 6, 7], 3))
    [[1, 2, 3], [4, 5, 6], [7]]

    :param iterable:
    :param batch_size:
    :return:
    """
    size = len(iterable)
    for count in range(0, size, batch_size):
        yield iterable[count:min(count + batch_size, size)]


class TaskPool(object):

    def __init__(self, pool_size=5):
        self._pool = ThreadPool(pool_size)
        self._tasks_args = list()

    @staticmethod
    def _task_function_wrapper(single_param):
        wrapped_task, wrapped_args, wrapped_kwargs = single_param
        return wrapped_task(*wrapped_args, **wrapped_kwargs)

    def add_task(self, task_function, *args, **kwargs):
        self._tasks_args.append((task_function, args, kwargs))

    def execute(self):
        logging.info('processing %d tasks', len(self._tasks_args))
        results = self._pool.map(TaskPool._task_function_wrapper, self._tasks_args)
        self._pool.close()
        self._pool.join()
        return results
