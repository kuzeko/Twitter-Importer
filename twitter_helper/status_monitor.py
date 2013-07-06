

class ProcessMonitor:
    """ Stub class to monitor the execution of the Threads """

    @staticmethod
    def print_messages(message_queue, logger=None):
        while True:
            message = message_queue.get()
            if message[0] > 0:
                if logger is not None:
                    logger.info(message[1])
                else:
                    print message[1]
            else:
                raise RuntimeError(message[1])
            message.task_done()
