import logging
import smart_open


class S3Handler(logging.StreamHandler):
    def __init__(self):
        pass

    def emit(self, record):
        try:
            self.format(record)
        except Exception as e:
            print(e)
            raise e
