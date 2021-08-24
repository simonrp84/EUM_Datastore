"""Utilities for accessing the EUMETSAT data store via API.
This module contains utility functions to assist with querying the
EUMETSAT data store and downloading the required data.

Copyright 2021 EUMETSAT, Ben Loveday, Simon Proud

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
documentation files (the "Software"), to deal in the Software without restriction, including without limitation
the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software,
and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions
of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED
TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF
CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
DEALINGS IN THE SOFTWARE.
"""

from datetime import datetime, timedelta
from urllib.parse import urljoin
from tqdm.auto import tqdm
import requests
import urllib
import os


def setup_debug():
    """A helper to enable debugging of the search / download procedure."""

    import logging
    logging.basicConfig()
    logging.getLogger().setLevel(logging.DEBUG)
    requests_log = logging.getLogger("requests.packages.urllib3")
    requests_log.setLevel(logging.DEBUG)
    requests_log.propagate = True


def default_services(api_endpoint: str = "http://api.eumetsat.int/") -> (str, str):
    """Return the default service URLs for EUMETSAT.

    Parameters:
        api_endpoint (string):
           The top level API URL. Defaults to EUMETSAT API.

    Returns:
        str, str:
           The search and download API URL endpoints.

    """

    # Searching endpoint
    service_search = api_endpoint + "data/search-products/os"

    # Downloading endpoint
    service_download = api_endpoint + "data/download/"

    return service_search, service_download


def get_pages(resp_json: requests.Response()):
    """Create iterator for looping over pages in results.

    The EUMETSAT results are spread across a number of pages with typically 10 results per page.
    To capture all results we have to iterate over the pages, which is set up here.

    Parameters:
        resp_json, requests.Response:
           A response from the server containing results.

    """

    n_items = resp_json['properties']['totalResults']
    items_page = resp_json['properties']['itemsPerPage']

    cur_item = 0
    while n_items > cur_item:
        yield cur_item
        cur_item += items_page


def find_files_on_store(start_date: datetime,
                        end_date: datetime,
                        search_url: str,
                        collection_id: str = "EO:EUM:DAT:MSG:HRSEVIRI",
                        bbox: tuple = None,
                        verbose: bool = False) -> list:
    """Retrieve details of files on the data store matching query.

    Parameters:
        start_date, datetime:
           Specifies earliest sensing time to search for.
        end_date, datetime:
           Specifies final sensing time to search for.
        collection_id, string:
           The name of the EUMETSAT collection to search for. Defaults to SEVIRI prime.
        bbox, list:
           Boundaries of the region to search for, most useful for polar satellites.
           This should be a list in format [[min_lon, min_lat], [max_lon, max_lat]]
           Default is full globe search.
        verbose, bool:
            Switch setting whether results are printed to screen.

    Returns:
        list:
            A list of filenames found on the server that correspond to the EUMETSAT search.

        """

    # Format our parameters for searching
    dataset_parameters = {'format': 'json', 'pi': collection_id,
                          'dtstart': start_date.strftime('%Y-%m-%dT%H:%M:%S.%fZ'),
                          'dtend': end_date.strftime('%Y-%m-%dT%H:%M:%S.%fZ')}
    if bbox is not None:
        dataset_parameters['bbox'] = '{},{},{},{}'.format(bbox[0][0], bbox[0][1], bbox[1][0], bbox[1][1])

    dataset_list = []
    # Initial request to find size
    tmp_response = requests.get(search_url, dataset_parameters).json()
    n_items = tmp_response['properties']['totalResults']
    progress_bar = tqdm()

    # Now we have to loop over all the pages
    for si_val in get_pages(tmp_response):
        dataset_parameters['si'] = si_val
        response = requests.get(search_url, dataset_parameters).json()
        for selected_data_set in response['features']:
            product_id = selected_data_set['properties']['identifier']
            dataset_list.append(product_id)
        progress_bar.update(n_items)
    progress_bar.close()

    if verbose:
        print(f'A total of {len(dataset_list)} files have been found.')

    return dataset_list


def get_token(access_key: str) -> dict:
    """Get the data download token from the user access_key supplied by EUM.

    Parameters:
        access_key, str:
            The access key given via the API user management portal on the EUMETSAT website.

    Returns:
        dict:
            A dict containing the download token returned by the EUMETSAT API.

    """
    base_url_wso2 = "http://api.eumetsat.int/"
    headers = {'Authorization': access_key}
    payload = {'grant_type': 'client_credentials'}
    url_ = urljoin(base_url_wso2, 'token')
    response = requests.post(url_, headers=headers, data=payload)
    tok = response.json()['access_token']
    token = {'access_token': tok}
    return token


def get_url_filename(dataset_list: list, local_dir: str, top_url: str) -> (str, str):
    """Generate remote url and local filename for download from a list of dataset filenames.

    Parameters:
        dataset_list, list:
            Remote server filenames retrieved via the API.
        local_dir, str:
            The local directory into which files will be saved.
        top_url:
            The download service URL for the EUMETSAT API.

    Returns:
        str:
            url of the file to download
        str:
            filename of the corresponding local file.
    """

    for product_id in dataset_list:
        local_filename = local_dir + product_id + '.zip'
        download_url = top_url + 'products/{}'.format(urllib.parse.quote(product_id))
        yield download_url, local_filename


def download_files(dataset_list: list,
                   remote_url: str,
                   output_dir: str,
                   eum_access_key: str = None,
                   verbose: bool = False,
                   block_size: int = 1024,
                   file_check_limit: int = 100000):
    """Download datasets from the store to local computer.

    Parameters:
        dataset_list, list:
            Names of the files to download from the data store, such as those from find_files_on_store()
        remote_url, str:
            The URL of the data store endpoint.
        output_dir, str:
            Directory name where downloaded files will be put.
        eum_access_key, str:
            The API access key defined on the EUMETSAT user portal.
        verbose, bool:
            Determines whether some additional progress information is printed to screen.
        block_size, int:
            Size in bytes of each downloaded chunk. Default is 1024 bytes.
        file_check_limit, int:
            Rough estimate of expected filesize for downloads in bytes,
            used to check if file downloaded successfully or if there may be an issue.
            Default is 100,000 bytes.

    """

    n_files = len(dataset_list)
    counter = 1

    if eum_access_key is None:
        try:
            eum_access_key = os.environ['EUM_ACCESS_KEY']
        except KeyError:
            raise KeyError("No EUMETSAT access key supplied. "
                           "Pass directly via `eum_access_key` or set the `EUM_ACCESS_KEY` environment variable.")

    access_key = 'Basic ' + eum_access_key
    access_token = get_token(access_key)
    if verbose:
        print("Retrieved access token from EUMETSAT.")
    initial_time = datetime.utcnow()

    for download_url, out_filename in tqdm(get_url_filename(dataset_list, output_dir, remote_url)):
        if verbose:
            print(f'Downloading file {counter} of {n_files}')
        counter += 1
        if os.path.exists(out_filename):
            print(f"File exists: {out_filename}")
            continue
        cur_time = datetime.utcnow()
        if cur_time > initial_time + timedelta(minutes=20):
            if verbose:
                print("Retrieving new token")
            access_token = get_token(access_key)
            initial_time = cur_time

        response = requests.get(download_url, params=access_token, stream=True)
        with open(out_filename, 'wb') as fd:
            for chunk in response.iter_content(chunk_size=block_size):
                fd.write(chunk)
        filesize = os.path.getsize(out_filename)
        if filesize < file_check_limit:
            print(f'ERROR: Bad file {out_filename}\n. Possible credential problem. Size: {filesize}')
            os.remove(out_filename)


def retrieve_collection_dict(api_endpoint: str = "http://api.eumetsat.int/") -> dict:
    """Download list of data collections via that API.

    Parameter:
        api_endpoint, str:
            The URL of the data store API, defaults to EUMETSAT.
    Returns:
        dict:
            Details of all the data collections available via the API.
    """
    # Retrieve all collections: size refers to the max number of returned results
    service_navigator = api_endpoint + "product-navigator/csw/record/_search"
    collection_parameters = {'_source_include': 'id,abstract', 'size': '500'}
    response = requests.get(service_navigator, params=collection_parameters)

    # build the json format responses into a list
    collection_list = {c['_source']['id']: c['_source']['abstract'] for c in response.json()['hits']['hits']}

    return collection_list
