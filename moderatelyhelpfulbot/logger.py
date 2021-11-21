import logging


def init_logger(logger_name, filename=None):
    import os

    if not filename:
        filename = os.path.join(logger_name + ".log")
    global logger
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)
    sh = logging.StreamHandler()
    sh.setLevel(logging.DEBUG)
    # create formatter
    # formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    # add formatter
    # logger.setFormatter(formatter)
    # sh.setFormatter(formatter)
    # add ch to logger
    if len(logger.handlers) == 0:
        # logger.addHandler(file_logger)
        logger.addHandler(sh)
    return logger


logger = init_logger("mhbot_log")
