# -*- coding: utf-8 -*-

from logging import getLogger
from migrate_tool import storage_service
from migrate_tool.task import Task

import oss2r
logger = getLogger(__name__)


class OssStorageService(storage_service.StorageService):

    def __init__(self, *args, **kwargs):

        endpoint = kwargs['endpoint']
        accesskeyid = kwargs['accesskeyid']
        accesskeysecret = kwargs['accesskeysecret']
        bucket = kwargs['bucket']
        self._oss_api = oss2r.Bucket(oss2r.Auth(accesskeyid, accesskeysecret), endpoint, bucket)
        self._prefix = kwargs['prefix'] if 'prefix' in kwargs else ''
        if self._prefix.startswith('/'):
            self._prefix = self._prefix[1:]

    def download(self, task, local_path):
        # self._oss_api.get_object_to_file(urllib.unquote(cos_path).encode('utf-8'), local_path)
        for i in range(20):
            logger.info("download task: %s, to local path: %s, with retry %d, need size: %d",
                        task.key, local_path, i, task.size)
            try:
                import os
                try:
                    # os.remove(task.key)
                    # todo, remove localpath first
                    os.remove(local_path)
                except Exception as e:
                    logger.error("before download, remove local path: %s, this is normal, warn: %s", local_path, str(e))
                    pass

                self._oss_api.get_object_to_file(task.key, local_path)
                if task.size is None:
                    logger.info("task: %s, size is None, skip check file size on local", task.key)
                    break

                from os import path
                if path.getsize(local_path) != int(task.size):
                    logger.error("download task: %s, to local path: %s, retry: %d, error, local size: %d, task size: %d",
                                 task.key, local_path, i, path.getsize(local_path), task.size)
                else:
                    logger.info("download task: %s, to local path: %s, retry: %d, success, break",
                                task.key, local_path, i)
                    break
            except Exception as e:
                logger.error("download task: %s, to local path: %s, retry: %d, error: %s",
                             task.key, local_path, i, str(e))
                pass
        else:
            raise IOError("download task: %s, to local path: %s, failed with 20 retry", task.key, local_path)

    def upload(self, cos_path, local_path):
        raise NotImplementedError

    def list(self):
        for obj in oss2r.ObjectIterator(self._oss_api, prefix=self._prefix):
            if obj.key[-1] == '/':
                continue
            logger.info("yield new object: {}".format(obj.key))
            yield Task(obj.key, obj.size, None)

    def restore(self, key):
        try:
            # restore archive object
            logger.debug("restore or check object: %s", key)
            # call oss api
            resp = self._oss_api.restore_object(key)
            # resp = self._oss_api._do("post", self._oss_api.bucket_name, key, params={"restore": ''})
            logger.debug("submit restore for object: %s, res status code: %d, headers: %s", key, resp.status, resp.headers)
            return resp.status
        except Exception as e:
            logger.exception(str(e))
        return None

    def exists(self, _path):
        raise NotImplementedError
