# -*- coding: utf-8 -*-
from __future__ import absolute_import
import pkg_resources
from ConfigParser import SafeConfigParser
from logging import getLogger, basicConfig, DEBUG
from sys import stderr
from argparse import ArgumentParser
import os
from os import path

from Queue import Empty, Full
import multiprocessing
import os
import signal
import time
import uuid

from migrate_tool.migrator import restore_check_thread
from migrate_tool.worker import work_thread

import signal
from logging.config import dictConfig
from threading import Thread
import sys
reload(sys)
sys.setdefaultencoding('utf8')


log_config = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'standard': {
            'format': '%(asctime)s - %(filename)s:%(lineno)s - %(process)d - %(name)s - %(message)s'
        },
        'error': {
            'format': '%(asctime)s\t%(message)s'
        }
    },
    'handlers': {
        'default': {
            'level': 'INFO',
            'formatter': 'standard',
            'class': 'logging.StreamHandler',
        },
        'error_file': {
            'level': 'INFO',
            'formatter': 'error',
            'class': 'logging.FileHandler',
            'filename': 'fail_files.txt',
            'mode': 'a'
        }
    },
    'loggers': {
        '': {
            'handlers': ['default'],
            'level': 'INFO',
            'propagate': True
        },
        'migrate_tool.fail_file': {
            'handlers': ['error_file'],
            'level': 'WARN',
            'propagate': False
        },
        'requests.packages': {
            'handlers': ['default'],
            'level': 'WARN',
            'propagate': True
        }
    }
}


services_ = {}


def loads_services():
    global services_
    for ep in pkg_resources.iter_entry_points(group='storage_services'):
        services_.update({ep.name: ep.load()})


def create_parser():
    parser_ = ArgumentParser()
    parser_.add_argument('-c', '--conf', type=file, required=True, help="specify your config")
    return parser_


def main_thread():
    pass


logger = getLogger(__name__)
fail_logger = getLogger('migrate_tool.fail_file')


def start_pool(threads_pool):
    logger.info("multiprocessing thread pool is staring")
    for p in threads_pool:
        p.start()
    logger.info("multiprocessing thread pool staring done")


def signal_pool(threads_pool):
    logger.info("multiprocessing thread pool signal begin")
    for p in threads_pool:
        os.kill(p.pid, signal.SIGUSR1)
    logger.info("multiprocessing thread pool signal done")


def wait_pool(threads_pool):
    logger.info("multiprocessing thread pool join begin")
    for p in threads_pool:
        p.join()
    logger.info("multiprocessing thread pool join done")


stop = False


def handler_stop(sig, frame):
    logger.info("main got signal: %d, means need stop", sig)
    global stop
    stop = True


def main_():
    # todo, add signal first
    signal.signal(signal.SIGINT, handler_stop)
    signal.signal(signal.SIGTERM, handler_stop)
    signal.signal(signal.SIGHUP, handler_stop)

    parser = create_parser()
    opt = parser.parse_args()
    conf = SafeConfigParser()
    conf.readfp(opt.conf)

    output_service_conf = dict(conf.items('source'))
    input_service_conf = dict(conf.items('destination'))
    if conf.has_option('common', 'threads'):
        _threads = conf.getint('common', 'threads')
    else:
        _threads = 10
    workspace_ = conf.get('common', 'workspace')
    try:
        os.makedirs(workspace_)
    except OSError:
        pass

    log_config['handlers']['error_file']['filename'] = path.join(workspace_, 'failed_files.txt')
    dictConfig(log_config)

    loads_services()
    output_service = services_[output_service_conf['type']](**output_service_conf)
    input_service = services_[input_service_conf['type']](**input_service_conf)
    work_dir = conf.get('common', 'workspace')

    # init share queue and lock, queue is for running task, lock is for leveldb filter
    share_queue = multiprocessing.Queue()
    lock = multiprocessing.Lock()

    # init restore process
    restore_process = multiprocessing.Process(target=restore_check_thread, name="restore_check_worker",
                                              args=(share_queue, lock, work_dir, output_service, input_service))
    restore_process.daemon = True
    restore_process.start()

    # init work process pool
    threads_pool = []
    limit = max([_threads, multiprocessing.cpu_count()])
    for i in range(limit):
        p = multiprocessing.Process(target=work_thread, name = "running_task_worker",
                                    args=(share_queue, lock, work_dir, output_service, input_service))
        p.daemon = True
        threads_pool.append(p)
    start_pool(threads_pool)

    while True:
        global stop
        if stop:
            logger.info("main process stop is true, will exit")
            break
        # check child process
        if restore_process.is_alive():
            logger.info("main process, sleep 3 seconds")
            time.sleep(3)
        else:
            logger.info("restore_check_process is not alive, maybe normally exit, "
                        "so main process will normally exit too")
            time.sleep(6)
            restore_process.join()
            signal_pool(threads_pool)
            wait_pool(threads_pool)
            logger.info("main process: %d, is exit normally", os.getpid())
            sys.exit(0)
        pass
    # todo, term signal quit, sleep a few more seconds
    time.sleep(6)
    restore_process.join()
    wait_pool(threads_pool)

    logger.info("main process: %d, is exit signal", os.getpid())
    pass


if __name__ == '__main__':
    main_()
