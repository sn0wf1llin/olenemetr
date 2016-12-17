import uuid
import ujson

import redis


class ResultsReader(object):

    def __init__(self, key, request_id, redis_pool, results_count=None):
        self.r_key = key
        self.request_id = request_id
        self.redis_pool = redis_pool
        self.results_count = results_count

    def read(self, timeout):
        result_data = []
        for i in range(self.results_count):
            data = self.redis_pool.blpop("%s:%s" % (self.r_key, self.request_id),
                                         timeout=timeout)
            if not data:
                raise RuntimeError("Job results reader timeout fail, cnt items: %s" % self.results_count)

            result_data.append(ujson.loads(data[1]))

        return result_data


class ResultsWriter(object):
    def __init__(self, key, request_id, redis_pool):
        self.r_key = key
        self.request_id = request_id
        self.redis_pool = redis_pool

    def write(self, data):
        self.redis_pool.lpush("%s:%s" % (self.r_key, self.request_id),
                              ujson.dumps(data))


class PubSubWriter(ResultsWriter):
    def write(self, data):
        self.redis_pool.publish("%s:%s" % (self.r_key, self.request_id),
                                data)


class RedisQueue(object):

    def __init__(self, name, host, port, pickler, r_pool_size=5, writer=None):
        self.name = name
        self.host = host
        self.port = port
        self.pickler = pickler
        self.r_pool_size = r_pool_size

        self.conn_write = None
        self.conn_read = None

        if writer is None:
            self.writer = ResultsWriter
        else:
            self.writer = writer

        self.handlers = {}

    def _init_connection(self):
        redis_params = {'host': self.host,
                        'port': int(self.port),
                        'max_connections': self.r_pool_size}

        self.conn_read = redis.Redis(**redis_params)
        self.conn_write = redis.Redis(**redis_params)

    def add_handler(self, job_type, func):
        if job_type in self.handlers:
            raise RuntimeError("Can't register more than one handler")

        self.handlers[job_type] = func

    def execute_handler(self, job_type, data):
        if isinstance(data, dict):
            return self.handlers[job_type](**data)
        else:
            return self.handlers[job_type](data)

    def add_jobs(self, job_type, job_data, chunk_size=1):
        request_id = str(uuid.uuid4())

        data_chunks = [job_data[x:x+chunk_size] for x in range(0, len(job_data), chunk_size)]

        for row in data_chunks:
            data = {'job': job_type,
                    'data': row,
                    'request_id': request_id}

            self.conn_write.lpush(redis_queue_key(self.name),
                                  self.pickler.dumps(data))

        return ResultsReader(redis_queue_key(self.name),
                             request_id,
                             self.conn_read,
                             len(data_chunks))

    def add_job(self, job_type, job_data):
        request_id = str(uuid.uuid4())

        data = {'job': job_type,
                'data': job_data,
                'request_id': request_id}

        self.conn_write.lpush(redis_queue_key(self.name), self.pickler.dumps(data))

        return ResultsReader(redis_queue_key(self.name),
                             request_id,
                             self.conn_read,
                             1)

    def reserve_job(self, wait_timeout):
        try:
            job_data = self.conn_read.blpop([redis_queue_key(self.name)], timeout=wait_timeout)
        except TimeoutError:
            return None, None, None

        if job_data is None:
            return None, None, None

        job_data = self.pickler.loads(job_data[1])

        writer = self.writer(redis_queue_key(self.name),
                             job_data['request_id'],
                             self.conn_write)

        return job_data['job'], job_data['data'], writer

# -- HELPERS --


def redis_queue_key(queue_name):
    return "rq::%s::job" % queue_name