import hashlib
import logging
import os
import re
import socket
import tempfile

import requests
from pywebhdfs.webhdfs import PyWebHdfsClient
from requests_kerberos import HTTPKerberosAuth, OPTIONAL


class HdfsRequestError(Exception):
    pass


class HdfsApi:
    def __init__(self, request_timeout=10, logger=logging, active_nn_host='localhost', kerberos=False):
        self.timeout = request_timeout
        self.hdfs_schema = os.environ.get('HDFS_NAMENODE_SCHEMA', 'http')
        self.hdfs_host = active_nn_host
        self.hdfs_port = os.environ.get('HDFS_NAMENODE_PORT', 50070)
        if kerberos:
            extra_opts = {'auth': HTTPKerberosAuth(mutual_authentication=OPTIONAL,
                                                   sanitize_mutual_error_response=False,
                                                   force_preemptive=True)}
        else:
            extra_opts = {}
        self.webhdfs = PyWebHdfsClient(host=self.hdfs_host, port=self.hdfs_port,
                                       request_extra_opts=extra_opts)
        self.logger = logger

    def request_namenode(self, path, method='GET', headers=None, **kwargs):
        self.logger.info("Calling HDFS API ({0})".format(path))
        if headers is None:
            headers = dict()

        if path.startswith('http'):
            hdfs_url = path
        else:
            hdfs_url = '{0}://{1}:{2}/{3}'.format(self.hdfs_schema, self.hdfs_host, self.hdfs_port, path)
        self.logger.debug(hdfs_url)
        r = requests.request(method, hdfs_url, headers=headers, timeout=self.timeout, verify=False,
                             auth=HTTPKerberosAuth(), **kwargs)
        return self._check_response_status(r)

    def request_webhdfs_status(self, path):
        return self.webhdfs.get_file_dir_status(path)

    def _check_response_status(self, response):
        self.logger.debug(response.text)
        if response.status_code >= 400:
            self.logger.error(
                "HdfsResponse returned with error status [{0}], response was: {1}".format(response.status_code,
                                                                                          response.text))
            raise HdfsRequestError("HdfsResponse returned with error status [{0}]".format(response.status_code))
        return response

    def get_block_info_for_file(self, file_path):
        path = "fsck"
        params = {'files': 0, 'racks': 1, 'blocks': 0, 'path': file_path}

        response = self.request_namenode(path, params=params)
        return response

    @staticmethod
    def get_first_block_info(filename, block_info):
        regex = r"^{0}.*\n(.*)\n".format(filename)
        info_of_first_block = re.findall(regex, block_info, re.MULTILINE)
        if len(info_of_first_block) < 1:
            raise HdfsRequestError("No block information found for file {0} in {1}".format(filename, block_info))
        return info_of_first_block[0]

    @staticmethod
    def get_location_of_first_block(block_info):
        ip_regex = r"(?<!\-)(\d{2,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})(?=:)"
        block_locations = re.findall(ip_regex, block_info)
        if len(block_locations) < 1:
            raise HdfsRequestError("No block location information found in {0}".format(block_info))
        return block_locations[0]

    @staticmethod
    def get_host_by_ip(ip):
        host_info = socket.gethostbyaddr(ip)
        if len(host_info) < 1:
            raise HdfsRequestError("Unable to get hostname form ip {0}".format(ip))
        return host_info[0]

    @staticmethod
    def calculate_md5(file, block_size=65536):
        hash_builder = hashlib.md5()
        for block in iter(lambda: file.read(block_size), b""):
            hash_builder.update(block)
        md5 = hash_builder.hexdigest()
        file.seek(0)
        return md5

    @staticmethod
    def create_temp_file():
        return tempfile.NamedTemporaryFile(suffix='.temporary', prefix='hdfs-smoketest-api-')

    def create_temp_file_of_size(self, temp_file_size):
        tmp = self.create_temp_file()
        tmp.seek(temp_file_size * 1024 * 1024)
        tmp.write(b'1')
        tmp.seek(0)

        return tmp

    def copy_to_hdfs(self, remote_path, tmpfile):
        self.webhdfs.create_file(remote_path, file_data=tmpfile, overwrite=True)

    def create_hdfs_file_of_size_in_mb(self, path, size=300):
        with self.create_temp_file_of_size(size) as tmp_file:
            md5_of_tmp_file = self.calculate_md5(tmp_file)
            self.copy_to_hdfs(path, tmp_file)

        return md5_of_tmp_file

    def get_remote_file(self, path):
        return self.webhdfs.read_file(path)

    def write_remote_file_to_local_temp(self, remote_path):
        local = self.create_temp_file()
        file = self.get_remote_file(remote_path)
        local.write(file)
        local.seek(0)
        return local

    def get_hdfsfile_and_calc_md5(self, path):
        with self.write_remote_file_to_local_temp(path) as temp_file:
            return self.calculate_md5(temp_file)

    def cleanup_remote_file(self, path, recursive=False):
        self.webhdfs.delete_file_dir(path, recursive=recursive)

    def get_host_location_of_first_block(self, filename):
        file_block_info = self.get_block_info_for_file(filename)
        file_first_block_info = self.get_first_block_info(filename, file_block_info.text)
        file_block_ip = self.get_location_of_first_block(file_first_block_info)
        return self.get_host_by_ip(file_block_ip)
