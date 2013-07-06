

class ProcessMonitor:
    """ Stub class to monitor the execution of the Threads """
    def print_messages(self, message_queue, logger=None):
        while True:
            message = messages.get()
            if message[0] > 0:
                if logger not None:
                    logger.info(message[1])
                else:
                    print message[1]
            else:
                raise raise RuntimeError(message[1])
            message.task_done()
