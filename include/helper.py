import time
import threading
import logging.config
from typing import Callable


class PipelineHelper:
    LOGGING_CONFIG = {
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'default': {
                'format': '%(asctime)s %(name)s - %(levelname)s - %(message)s',
                'datefmt': '%m-%d %H:%M',
            },
        },
        'handlers': {
            'file': {
                'class': 'logging.FileHandler',
                'formatter': 'default',
                'level': 'INFO',
                'filename': '../logs/pipeline.log',
            },
            'console': {
                'class': 'logging.StreamHandler',
                'formatter': 'default',
                'level': 'INFO',
            },
        },
        'root': {
            'handlers': ['file', 'console'],
            'level': 'INFO',
        },
    }

    def __init__(self):
        self.setup_logging()
        self.logger = logging.getLogger(__name__)

    @classmethod
    def setup_logging(cls):
        logging.config.dictConfig(cls.LOGGING_CONFIG)

    def run_using_threads(self, tables: list, function_obj: Callable[[str], None]):
        threads = []
        start_time = time.perf_counter()

        for filename in tables:
            thread = threading.Thread(target=function_obj, args=(filename,))
            thread.start()
            threads.append(thread)

        for thread in threads:
            thread.join()

        end_time = time.perf_counter()
        self.logger.info(f"Finished {function_obj.__name__} job")
        self.logger.info(f"Took: {(end_time - start_time):.2f}s")

