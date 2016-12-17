"""
    QueueProcessor is used to process queues.
    Do note that the processing of actual jobs is done in `queue_handler`.
"""
import logging
import asyncio
import traceback
import signal
from .controller import get_queue

logger = logging.getLogger()

WAIT_TIMEOUT = 1  # 1 sec


class QueueProcessor(object):

    def __init__(self, queue_name):
        self.__stop_processor = False
        self.queue = get_queue(queue_name)

    def stop_processor(self, signum, frame):
        self.__stop_processor = True

    def process(self, debug=False):
        signal.signal(signal.SIGTERM, self.stop_processor)

        print("start listen:", self.queue.name)
        while True:
            if self.__stop_processor:
                print("process canceled by SIGTERM")
                break

            #try:
            job, job_data, writer = self.queue.reserve_job(wait_timeout=WAIT_TIMEOUT)
            # except Exception as e:
            #     logger.error(e)
            #     traceback.print_exc()
            #     continue

            if job is None:
                continue

            logger.debug('Processing %s', job)

            # process the job
            #try:
            result_data = self.queue.execute_handler(job, job_data)
            writer.write(result_data)

            #except Exception as e:
            #    logger.error(e)
            #    traceback.print_exc()
            #    continue


