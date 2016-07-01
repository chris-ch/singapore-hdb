import logging
from multiprocessing.pool import ThreadPool


class TaskPool(object):

    def __init__(self, pool_size=5):
        self._pool = ThreadPool(pool_size)
        self._tasks_args = list()

    @staticmethod
    def _task_function_wrapper(single_param):
        wrapped_task, wrapped_task_id, wrapped_args, wrapped_kwargs = single_param
        try:
            result = wrapped_task(*wrapped_args, **wrapped_kwargs)

        except Exception as err:
            logging.error('task %d failed', err)
            raise

        return result

    def add_task(self, task_function, *args, **kwargs):
        task_id = len(self._tasks_args) + 1
        self._tasks_args.append((task_function, task_id, args, kwargs))

    def execute(self):
        logging.info('processing %d tasks', len(self._tasks_args))
        results = self._pool.map(TaskPool._task_function_wrapper, self._tasks_args)
        self._pool.close()
        self._pool.join()

        # serialized version below
        #results = list()
        #for task_function, task_id, args, kwargs in self._tasks_args:
        #    result = task_function(*args, **kwargs)
        #    results.append(result)

        return results
