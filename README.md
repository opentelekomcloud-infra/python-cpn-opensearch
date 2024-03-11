# python-cpn-opensearch
The python tool fetches information for our Circle Partner Navigator from Strapi backend and pushes the info to the Opensearch engine.

## Installation

```
git clone git@github.com:opentelekomcloud-infra/python-cpn-opensearch.git
cd ./python-cpn-opensearch
pip install .
````

## Sample command

```
cpnsearch --hosts 'opensearch.eco.tsi-dev.otc-service.com:443' --user <opensearch_user> --password <opensearch_password> --strapi-url <strapi_base_url> --strapi-token <strapi_token> --delete-indices
```
