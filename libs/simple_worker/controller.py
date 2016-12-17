
#--- Global ----------------------------------------------
QUEUES = {}


#--- Accessor and setup of queues ----------------------------------------------
def get_all_queues():
    return QUEUES


def get_queue(queue_name):
    return QUEUES.get(queue_name)


def setup_queue(queue_name, host, port, pickler, pool_size=5, writer=None):
    global QUEUES

    from .queue import RedisQueue

    if queue_name not in QUEUES:
        queue = RedisQueue(queue_name, host, port, pickler=pickler, r_pool_size=pool_size,
                           writer=writer)
        queue._init_connection()

        QUEUES[queue_name] = queue

    return QUEUES.get(queue_name)


#--- Decorator for more beautiful handling of the queue
def wrap_queue_handler(queue_name, job_type):
    def wrapper(func):
        queue = get_queue(queue_name)
        if not queue:
            raise Exception('Queue %s not found' % queue_name)
        queue.add_handler(job_type, func)
        return func
    return wrapper


__all__ = ['get_queue', 'setup_queue',
           'wrap_queue_handler',]
