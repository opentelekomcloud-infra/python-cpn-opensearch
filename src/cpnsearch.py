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
import os
import sys
import logging

from src.common.clients import Searchclient
import requests

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
        '--locales',
        metavar='<locales>',
        nargs='+',  # List of locale elements
        default=['en', 'de-DE'],
        help="OpenSearch locales\n"
            'Default: ["en", "de-DE"]'
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
        help='Base URL to Strapi'
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


def strapi_request(strapi_token, strapi_url, page, locale):
    url = (strapi_url + '/api/partners?locale=' + locale + '&populate'
           '=features&populate=tags&populate=quotes&sort[0]=partner_id&'
           'pagination[pageSize]=5&pagination[page]=' + str(page))
    headers = {
        'Authorization': 'Bearer ' + strapi_token
    }
    try:
        response = requests.get(url, headers=headers)
    except requests.exceptions.RequestException as e:
        sys.exit("Issue while Strapi request: ", e)
    return response.json()


def index_data(client, index_prefix, strapi_url, strapi_token, locale):
    language_code = locale.split('-')[0]
    index_name = "partners-" + language_code
    index = index_prefix + index_name
    responses = []
    data = {}
    page = 1
    not_finished = True

    while not_finished:
        data = strapi_request(
            strapi_token=strapi_token,
            strapi_url=strapi_url,
            page=page,
            locale=locale)
        bulk_data = []

        # check for existing data
        if len(data["data"]) > 0:
            # format retrieved partners and prepare for bulk POST request
            for partner in data["data"]:
                partner_id = partner["attributes"]["partner_id"]
                create_command = {"create": {
                    "_index": index,
                    "_id": partner_id
                }
                }
                bulk_data.append(create_command)
                bulk_data.append(partner)
            # Bulk operation to upload multiple partners
            try:
                responses.append(client.bulk(bulk_data))
            except Exception as e:
                sys.exit("Issue while bulk request: ", e)
        else:
            sys.exit("No data found")

        page_count = data["meta"]["pagination"]["pageCount"]
        if page_count == page:
            not_finished = False
        page = page + 1

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

    logging.basicConfig(level=logging.INFO)
    if args.debug:
        logging.basicConfig(level=logging.DEBUG)

    client = get_client(user, args.hosts)

    if args.delete_indices:
        delete_indices(client=client, index_prefix=args.index_prefix)
    
    if args.locales:
        locales = args.locales
        print('Indexing data for locales: ', locales) 
        for locale in locales:
            response = index_data(
                client=client,
                index_prefix=args.index_prefix,
                strapi_url=args.strapi_url,
                strapi_token=args.strapi_token,
                locale=locale
            )
            logging.info(response)
    else:
        sys.exit('No locales specified.')

if __name__ == "__main__":
    main()
