# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
import sys

from opensearchpy import helpers
from opensearchpy import OpenSearch


def generate_os_host_list(hosts):
    host_list = []
    for host in hosts:
        raw_host = host.split(':')
        if len(raw_host) != 2:
            raise Exception('--hosts parameter does not match the following '
                            'format: hostname:port')
        json_host = {'host': raw_host[0], 'port': int(raw_host[1])}
        host_list.append(json_host)
    return host_list


def create_index(client, json_list, index):
    try:
        response = helpers.bulk(
            client,
            json_list,
            index=index
        )
    except Exception as e:
        sys.exit("\nERROR:\n" + str(e))
    return response


class Searchclient:

    def __init__(self, username, password, hosts):
        self.username = username
        self.password = password
        self.hosts = hosts

    def connect(self):
        hosts = generate_os_host_list(self.hosts)
        client = OpenSearch(
            hosts=hosts,
            http_compress=True,
            http_auth=(self.username, self.password),
            use_ssl=True,
            verify_certs=True,
            ssl_assert_hostname=False,
            ssl_show_warn=False
        )
        return client
