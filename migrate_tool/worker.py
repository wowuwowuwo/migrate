# -*- coding:utf-8 -*-
from os import path, makedirs
from logging import getLogger
import time
from Queue import Empty, Full
import multiprocessing
import os
import uuid
import signal

from migrate_tool.filter import Filter


logger = getLogger(__name__)
fail_logger = getLogger('migrate_tool.fail_file')
pool_stop = False
restore_process_finish = False


def handler_user1(sig, frame):
    logger.info("got signal: %d, means restore process is finish", sig)
    global restore_process_finish
    restore_process_finish = True


def handler_stop(sig, frame):
    logger.info("got signal: %d, means need stop", sig)
    global pool_stop
    pool_stop = True


def work_thread(share_queue, lock, work_dir, output_service, input_service):
    logger.info("multiprocessing pool worker me: %d, is starting", os.getpid())
    # _filter = Filter(work_dir)

    # todo, signal
    signal.signal(signal.SIGUSR1, handler_user1)

    # signal.signal(signal.SIGINT, handler_stop)
    # signal.signal(signal.SIGTERM, handler_stop)
    # signal.signal(signal.SIGHUP, handler_stop)
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    signal.signal(signal.SIGTERM, signal.SIG_DFL)
    signal.signal(signal.SIGHUP, signal.SIG_DFL)

    while True:
        global pool_stop
        if pool_stop:
            logger.info("pool_stop is true, work process: %d, will exit", os.getpid())
            break

        # check restore process is finish or not
        global restore_process_finish
        if restore_process_finish and share_queue.empty():
            logger.info("restore process is finish, and share task queue is empty, pool worker me: %d will exit",
                        os.getpid())
            break

        try:
            logger.info("get one task from running task queue begin")
            task = share_queue.get_nowait()
        except Empty:
            logger.info("task queue is empty, will sleep 3 seconds")
            time.sleep(3)
            continue

        logger.info("finally get one task from running task queue done, task: %s", task.key)

        task_path = task.key
        if task_path.startswith('/'):
            task_path = task_path[1:]
        if isinstance(task_path, str):
            task_path = task_path.decode('utf-8')

        localpath = unicode(path.join(work_dir, uuid.uuid4().hex))
        try:
            try:
                os.makedirs(path.dirname(localpath))
            except OSError as e:
                # directory is exists
                logger.warn("local path: %s, exists, possible work dir ! warn: %s", localpath, str(e))

            # step 1, check exists
            try:
                ret = input_service.exists(task)
                if ret:
                    logger.info("finally check task: %s, exists, skip it", task_path.encode('utf-8'))
                    # with lock:
                    #     _filter.add(task_path)
                    continue
                else:
                    logger.info("finally check task: %s, not exists, will download it to local path: %s",
                                task_path.encode('utf-8'), localpath)
            except Exception as e:
                # todo, override ?
                logger.exception("finally check task: %s, exists failed, error: %s", task_path.encode('utf-8'), str(e))

            # step 2, download file
            try:
                output_service.download(task, localpath)
            except Exception as e:
                logger.exception("finally download task: %s, to local path: %s, failed, error: %s",
                                 task_path.encode('utf-8'), localpath, str(e))
                fail_logger.error(task_path)
                # with lock:
                #     fail += 1
                continue
            logger.info("finally download task: %s, success, size: %d, to local path: %s", task.key, task.size,
                        localpath)

            # step 3, upload file
            try:
                input_service.upload(task, localpath)
            except Exception as e:
                logger.exception("finally upload task: %s, from local path: %s, failed, error: %s",
                                 task_path.encode('utf-8'), localpath, str(e))
                # with lock:
                #     fail += 1
                fail_logger.error(task_path)
                continue
            logger.info("finally upload task: %s, success, size: %d, from local path: %s", task.key, task.size,
                        localpath)

            # step 4, remove tmp file
            try:
                if isinstance(localpath, unicode):
                    localpath = localpath.encode('utf-8')
                os.remove(localpath)
                try:
                    os.removedirs(path.dirname(localpath))
                except OSError:
                    pass
            except Exception as e:
                logger.exception("finally remove task tmp file: %s, failed, size: %d, local path: %s, error: %s",
                                 task.key, task.size, localpath, str(e))
                continue
            logger.info("finally remove task tmp file: %s, success, size: %d, local path: %s", task.key, task.size,
                        localpath)
            # todo, check task etag between source and destination
            # if isinstance(task_path, unicode):
            #     logger.info("inc succ with {}".format(task_path.encode('utf-8')))
            # else:
            #     logger.info("inc succ with {}".format(task_path.encode('utf-8')))

            # with lock:
            #     add task to filter
            #     succ += 1
            #     _filter.add(task_path)
        except Exception as e:
            logger.exception("try except for deleting file, error: %s", str(e))
        finally:
            if isinstance(localpath, unicode):
                localpath = localpath.encode('utf-8')
            try:
                os.remove(localpath)
                os.removedirs(path.dirname(localpath))
            except OSError:
                pass
        pass
    logger.info("multiprocessing pool worker: %d, will exit", os.getpid())
    pass


