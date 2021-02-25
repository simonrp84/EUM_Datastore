"""A script for using the EUMETSAT data store API to search and download satellite data.


Copyright 2021 Simon Proud

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

from datetime import datetime, timezone
import DS_Utils as DSU
import click


@click.command()
@click.argument('start_dt')
@click.argument('end_dt')
@click.option('--min_lon', default=None)
@click.option('--max_lon', default=None)
@click.option('--min_lat', default=None)
@click.option('--max_lat', default=None)
@click.option('--out_dir', default='./')
@click.option('--collection', default='EO:EUM:DAT:METOP:IASIL1C-ALL')
@click.option('--eum_access_key', default=None)
def main(start_dt: datetime,
         end_dt: datetime,
         min_lon: int,
         max_lon: int,
         min_lat: int,
         max_lat: int,
         out_dir: str,
         collection: str,
         eum_access_key: str):
    """Search for data on the EUMETSAT data store and download.

    Command line arguments:\n
      start_dt: String time for the start of the search in YYYYMMDDHHMM or YYYYMMDD format.\n
      end_dt: String time for the end of the search in YYYYMMDDHHMM or YYYYMMDD format.\n
    Optional arguments\n
      --collection: String defining the collection to search/download. Default: EO:EUM:DAT:METOP:IASIL1C-ALL (IASI L1C)\n
      --min_lon: Minimum longitude of search box. Default: None\n
      --max_lon: Maximum longitude of search box. Default: None\n
      --min_lat: Minimum latitude of search box. Default: None\n
      --max_lat: Maximum latitude of search box. Default: None\n
      --out_dir: Output directory in which to save files. Default: Current directory\n
      --eum_access_key: The EUMETSAT access key as defined on the data store user page. Default: None\n

    Examples:\n
        Search and download SEVIRI RSS data for morning of 5th Jan 2021, saving to network:\n
        python Datastore_Search_Download.py 202101050000 202101051200 --out_dir=/gf3/sat_data/some_dir/ --collection=EO:EUM:DAT:MSG:MSG15-RSS\n

        Search and download IASI L1C for a region around Etna in May 2020:\n
        python Datastore_Search_Download.py 202005010000 202005312359 --min_lon=14 --max_lon=15 --min_lat=14 --max_lat=16\n

    """
    try:
        start_dt = datetime.strptime(start_dt, '%Y%m%d%H%M').replace(tzinfo=timezone.utc)
    except ValueError:
        try:
            start_dt = datetime.strptime(start_dt, '%Y%m%d').replace(tzinfo=timezone.utc)
        except ValueError:
            raise ValueError("Start date/time must be in YYYYMMDD or YYYYMMDDHHMM format.")

    try:
        end_dt = datetime.strptime(end_dt, '%Y%m%d%H%M').replace(tzinfo=timezone.utc)
    except ValueError:
        try:
            end_dt = datetime.strptime(end_dt, '%Y%m%d').replace(tzinfo=timezone.utc)
        except ValueError:
            raise ValueError("End date/time must be in YYYYMMDD or YYYYMMDDHHMM format.")

    # Get API details
    service_search, service_download = DSU.default_services()

    # Setup bounding box
    if not [x for x in (min_lon, min_lat, max_lon, max_lat) if x is None]:
        search_bbox = [[min_lon, min_lat], [max_lon, max_lat]]
    else:
        search_bbox = None

    # Search for datasets:
    datasets = DSU.find_files_on_store(start_dt,
                                       end_dt,
                                       service_search,
                                       collection,
                                       bbox=search_bbox)
    print(f"A total of {len(datasets)} datasets have been found.")

    DSU.download_files(datasets,
                       service_download,
                       output_dir=out_dir,
                       eum_access_key=eum_access_key,
                       verbose=True)


if __name__ == '__main__':
    main()
