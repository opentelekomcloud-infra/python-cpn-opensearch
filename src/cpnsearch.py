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
import argparse
import json
import os
import sys
import logging

from src.common.clients import create_index
from src.common.clients import Searchclient

# https://strapi.partners.otc-service.com/api/partners?populate=localizations&populate=features.localizations&populate=tags&populate=quotes.localizations&sort[0]=partner_id&pagination[pageSize]=5&pagination[page]=2

def get_parser():
    # Format the output of help
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument(
        '--debug',
        action='store_true',
        help='Option enables Debug output.'
    )
    parser.add_argument(
        '--delete-indices',
        action='store_true',
        help='Option deletes old index with the same name and creates new '
             'one.'
    )
    parser.add_argument(
        '--hosts',
        metavar='<host:port>',
        nargs='+',
        default=['localhost:9200'],
        help='Provide one or multiple host:port values for Opensearch '
             'separated by space for multiple entries.\n'
             'Default: localhost:9200'
    )
    parser.add_argument(
        '--index-prefix',
        metavar='<index_prefix>',
        default='cpn-',
        help="OpenSearch index prefix.\n"
             'Default: cpn-'
    )
    parser.add_argument(
        '--password',
        metavar='<password>',
        help='Password for the Opensearch connection.'
    )
    parser.add_argument(
        '--post-count',
        metavar='<count>',
        default=5,
        type=int,
        help='Number of files being loaded for indexing at the same time.\n'
             'Default: 5'
    )
    parser.add_argument(
        '--strapi-token',
        metavar='<strapi_token>',
        help='Token to connect Strapi'
    )
    parser.add_argument(
        '--strapi-url',
        metavar='<strapi_url>',
        help='URL to Strapi API'
    )
    parser.add_argument(
        '--user',
        metavar='<username>',
        help='Username for the Opensearch connection.'
    )

    args = parser.parse_args()
    return args


def get_user(args):
    user = {
        'name': '',
        'password': ''
    }
    user['name'] = os.environ.get('SEARCH_USER')
    user['password'] = os.environ.get('SEARCH_PASSWORD')
    if not user['name']:
        if args.user:
            user['name'] = args.user
        else:
            raise Exception('SEARCH_USER environment variable or --user '
                            'parameter do not exist.')
    if not user['password']:
        if args.password:
            user['password'] = args.password
        else:
            raise Exception('SEARCH_PASSWORD environment variable or '
                            '--password parameter do not exist.')
    return user


def delete_indices(client, index_prefix):
    try:
        # delete indices with wildcard
        client.indices.delete(index=index_prefix + '*', ignore=[400, 404])
    except Exception as e:
        sys.exit('Exception raised while indices deletion:\n' + str(e))

def create_index_data(client, index_prefix):
    responses = []
    data = {}
    data["body"] = "hallo Sebastian"

    resp = client.index(
        index = (index_prefix + '1'),
        body = data,
        id = 'axxon',
        # refresh = True
    )
    responses.append(resp)

    json_response = {
        'responses': responses
    }
    return json_response

def get_client(user, hosts):
    client = Searchclient(
        username=user['name'],
        password=user['password'],
        hosts=hosts
    )
    return client.connect()

def main():
    args = get_parser()
    user = get_user(args)

    if args.debug:
        logging.basicConfig(level=logging.DEBUG)

    client = get_client(user, args.hosts)

    if args.delete_indices:
        delete_indices(client=client, index_prefix=args.index_prefix)

    response = create_index_data(
        client=client,
        index_prefix=args.index_prefix,
    )

    logging.info(response)


if __name__ == "__main__":
    main()
