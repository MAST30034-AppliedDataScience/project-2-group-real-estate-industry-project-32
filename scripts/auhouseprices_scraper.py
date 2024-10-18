import asyncio
from multiprocessing.managers import DictProxy
import traceback
import aiohttp
import pickle
import multiprocessing
from multiprocessing import Manager, Pool
import csv
import random
import socket
from bs4 import BeautifulSoup
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn
from rich.table import Table
import xml.etree.ElementTree as ET
import regex as re
import numpy as np
from numpy.dtypes import StringDType
import time
import gzip
import shutil
import hashlib
import pprint
import os
import sys


def check_make_dir(directory: str):
    '''
    Function to check if the directory exists and create it if it doesn't
    '''
    os.makedirs(directory, exist_ok=True)

def get_suffix(url: str):
    '''
    Function to extract the suffix from the URL
    '''
    return url.split('VIC/')[1]

# Utilities from https://stackoverflow.com/questions/34710835/proper-way-to-shutdown-asyncio-tasks
def _get_last_exc():
    exc_type, exc_value, exc_traceback = sys.exc_info()
    sTB = '\n'.join(traceback.format_tb(exc_traceback))
    return f"{exc_type}\n - msg: {exc_value}\n stack: {sTB}"


def _exit_program(code=1):

    # kill all active asyncio Tasks
    if asyncio.Task:
        for task in asyncio.Task.all_tasks():
            try:
                task.cancel()
            except Exception as ex:
                pass

    # flush stderr and stdout
    sys.stdout.flush()
    sys.stderr.flush()

    # Shut down
    sys.exit(code)

async def get_proxy_url_brightdata(country=None, return_session=False):
    '''
    Function to get the proxy URL
    '''
    # Random country from the country list
    if not country:
        username = 'REDACTED'
    else:
        username = 'REDACTED'
    password = 'REDACTED'

    async with asyncio.Lock():
        if username == 'REDACTED':
            print("Please enter your proxy auth details username and password in the get_proxy_url_brightdata function")
            _exit_program(0)
        

    port = 22225

    while True:
        try:
            session_id = str(random.random())
            super_proxy = socket.gethostbyname('brd.superproxy.io')
            proxy_url = f"http://{username}-session-{session_id}:{password}@{super_proxy}:{port}"
            if return_session:
                return f"{username}-session-{session_id}"
            return proxy_url
        except socket.gaierror as e:
            time.sleep(1)  # Wait for a second before retrying

class AuHousePricesScraper:
    def __init__(self, phase: int = 1):
        self.script_dir = os.path.dirname(os.path.abspath(__file__))
        self.base_landing_dir = os.path.abspath(os.path.join(self.script_dir, '../data/landing/'))
        self.auhouseprices_landing_dir = os.path.abspath(os.path.join(self.script_dir, "../data/landing/auhouseprices/"))
        self.base_raw_dir = os.path.abspath(os.path.join(self.script_dir, '../data/raw_test/'))
        self.auhouseprices_raw_dir = os.path.abspath(os.path.join(self.script_dir, "../data/raw/auhouseprices_test/"))
        self.tmp_dir = os.path.abspath(os.path.join(self.script_dir, '../data/landing/tmp/'))

        self.sitemap_url = "https://www.auhouseprices.com/sitemap.xml"
        self.base_url = "https://www.auhouseprices.com/suburb/market/state/VIC/"
        self.rent_list_url = "https://www.auhouseprices.com/rent/list/VIC/"

        self.phase = phase
        self.lock = asyncio.Lock()

    def start(self):
        check_make_dir(self.auhouseprices_landing_dir)
        check_make_dir(self.base_landing_dir)
        check_make_dir(self.base_raw_dir)
        check_make_dir(self.auhouseprices_raw_dir)
        check_make_dir(self.tmp_dir)


        self.manager = Manager()
        self.shared_data = self.manager.dict()
        self.shared_data['num_file_already_seen'] = 0
        self.shared_data['num_scraped'] = 0
        self.shared_data['num_parsed'] = 0
        self.shared_data['num_extra_urls'] = 0
        self.shared_data['extra_urls'] = self.manager.list()
        self.shared_data['extra_urls'].append(set())
        self.shared_data['rental_listing_urls'] = self.manager.list()
        self.shared_data['rental_listing_urls'].append(set())
        self.shared_progress = self.manager.dict()
        self.shared_progress['completed'] = 0
        self.multiprocessing_lock = None
        self.address_erroneous_files = False
        self.landing_dir_files = [file for file in os.listdir(self.auhouseprices_landing_dir) if file.endswith(".html")]


        if self.phase == 2:
            return self.landing_dir_files

        elif self.phase == 3:
            self.address_erroneous_files = True
            asyncio.run(self.address_erroneous_urls())
            # Landing dir files for this are those from all_erroneous_files.txt file in the auhouseprices_raw_dir
            with open(os.path.join(self.auhouseprices_raw_dir, 'all_erroneous_files.txt'), 'r') as f:
                landing_dir_files = f.read().splitlines()
                return landing_dir_files
        else:
            asyncio.run(self.main(self.shared_data))
            landing_dir_files = [file for file in os.listdir(self.auhouseprices_landing_dir) if file.endswith(".html")]



    def get_proxy_url(self, proxy_list: list):
        '''
        Function to get the proxy URL
        '''
        return f'http://{random.choice(proxy_list)}'


    async def get_url(self, url: str, purpose: str, max_retries=150, use_proxy=True):
        '''
        Function to get the URL using aiohttp
        '''
        headers = {
            "Accept": (
                "text/html,application/xhtml+xml,application/xml;q=0.9,"
                "image/avif,image/webp,image/apng,*/*;q=0.8,"
                "application/signed-exchange;v=b3;q=0.7"
            ),
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/128.0.0.0 Safari/537.36 Edg/128.0.0.0"
            )
        }
        while True:
            if use_proxy:
                proxy_url = await get_proxy_url_brightdata()
            else:
                proxy_url = None
            async with aiohttp.ClientSession() as session:
                try:
                    async with session.get(url, headers=headers, proxy=proxy_url) as response:
                        if response.status == 200:
                            content = await response.read()
                            if purpose == "return_soup_object":
                                soup = BeautifulSoup(content, 'lxml')
                                return soup
                            else:
                                return content
                        else:
                            if use_proxy:
                                continue  # Retry with a new proxy
                            else:
                                break
                except Exception as err:
                    # print(f'get_url: {err}')
                    if use_proxy:
                        continue  # Retry with a new proxy
                    else:
                        break
        print(f"All attempts failed for URL: {url}")
        return None

    async def scrape_listings(self, partition: list, progress_bar_dict: dict, shared_data: DictProxy):
        '''
        Function to parse the listings of each partition
        '''
        # Initialize the progress bar
        progress = progress_bar_dict['progress']
        progress.update(task_id=progress_bar_dict['task_id'], advance=0)

        for url in partition:
            # Extract unique ID from the URL
            id_match = re.search(r'/rent/view/VIC/(\d+)/', url)
            unique_id = int(id_match.group(1)) if id_match else None
            if not unique_id:
                continue
            # Use the self.lock to ensure that only one thread is accessing the directory at a time.
            async with self.lock:
                # Check if .html file already exists
                input_file = os.path.join(self.auhouseprices_landing_dir, f"{unique_id}.html")
                if os.path.exists(input_file):
                    progress.update(task_id=progress_bar_dict['task_id'], advance=1)
                    shared_data["num_file_already_seen"] += 1
                    if shared_data["num_file_already_seen"] % 100000 == 0:
                        print(f'Already seen {shared_data["num_file_already_seen"]} listings')
                    continue

            # Request the listing page
            html_obj = await self.get_url(url, "return_response_object")
            async with self.lock:
                if html_obj:
                    # Write the .html file to the directory
                    output_file = os.path.join(self.auhouseprices_landing_dir, f"{unique_id}.html")
                    with open(output_file, 'wb') as f:
                        f.write(html_obj)
                    shared_data['num_scraped'] += 1
            if unique_id and html_obj:
                progress.update(task_id=progress_bar_dict['task_id'], advance=1)

    async def address_erroneous_urls(self):
        '''
        Address erroneous .html listings by re-scraping the URLs,
        compare checksums of the re-scraped .html with the original .html,
        attempt to re-parse the content and save the parsed listings to a new partioned
        .csv file
        '''
        all_erroneous_ids = []

        # Iterate through each erroneous_url_partition-{partition_idx}.txt file in the auhouseprices_raw_dir
        for file in os.listdir(self.auhouseprices_raw_dir):
            if file.startswith('erroneous_urls_partition-') and file.endswith('.txt'):
                erroneous_urls_file = os.path.join(self.auhouseprices_raw_dir, file)
                with open(erroneous_urls_file, 'r', encoding='utf-8') as f:
                    ids = f.readlines()
                    ids = [int(id.strip().replace('.html', '')) for id in ids]
                    all_erroneous_ids.extend(ids)

        # Write an all_erroneous_files.txt file to the auhouseprices_raw_dir, containing all the erroneous filenames
        all_erroneous_files_file = os.path.join(self.auhouseprices_raw_dir, 'all_erroneous_files.txt')
        with open(all_erroneous_files_file, 'w', encoding='utf-8') as f:
            for id in all_erroneous_ids:
                f.write(f"{id}.html\n")

        # Re-scrape the erroneous URLs which contain the IDs in all_ids, from the all_urls.txt file
        all_urls_file = os.path.join(self.auhouseprices_landing_dir, 'all_urls.txt')
        with open(all_urls_file, 'r') as f:
            all_urls = f.readlines()
            all_urls = [url.strip() for url in all_urls]
            id_url_map = {int(re.search(r'/rent/view/VIC/(\d+)/', url).group(1)): url for url in all_urls}

            # Write an all_erroneous_urls.txt file to the auhouseprices_raw_dir, containing all the erroneous URLs
            all_erroneous_urls_file = os.path.join(self.auhouseprices_raw_dir, 'all_erroneous_urls.txt')
            with open(all_erroneous_urls_file, 'w', encoding='utf-8') as f:
                for id in all_erroneous_ids:
                    f.write(f"{id_url_map[id]}\n")
                print(f"Total number of erroneous URLs: {len(all_erroneous_ids)}")

            with Progress(
                SpinnerColumn(),
                BarColumn(),
                TextColumn("[progress.percentage]{task.percentage:>3.1f}%"),
                TextColumn("{task.completed}/{task.total} listings processed"),
            ) as progress:
                task = progress.add_task("Erroneous listings...", total=len(all_erroneous_ids))
                progress_bar_dict = {
                    'progress': progress,
                    'num_overwritten': 0,
                    'num_identical': 0
                }
                progress_bar_dict['task_id'] = task

                # Determine the maximum length of the URLs
                max_url_length = max(len(url) for url in id_url_map.values())

                # Create the structured array with the appropriate dtype
                # Filter id_url_map to only include entries corresponding to all_erroneous_ids
                filtered_id_url_map = {id: id_url_map[id] for id in all_erroneous_ids if id in id_url_map}

                if not filtered_id_url_map:
                    return
                # Determine the maximum length of the URLs in the filtered map
                max_url_length = max(len(url) for url in filtered_id_url_map.values())

                # Create the structured array with the appropriate dtype
                partitions = np.array(list(filtered_id_url_map.items()), dtype=[('id', int), ('url', f'S{max_url_length}')])
                partitions = np.array_split(partitions, 450)

                parsed_listings = []

                tasks = []
                for partition in partitions:
                    task = asyncio.create_task(
                        self.verify_erroneous_listings(partition, parsed_listings, progress_bar_dict)
                    )
                    tasks.append(task)

                await asyncio.gather(*tasks)

                print(f"Number of listings overwritten: {progress_bar_dict['num_overwritten']}")
                print(f"Number of listings identical: {progress_bar_dict['num_identical']}")


    async def verify_erroneous_listings(self, partition: np.ndarray, parsed_listings: list, progress_bar_dict: dict):
        '''
        Verify the erroneous listings by re-scraping their respective URLs
        '''
        # Initialize the progress bar
        progress = progress_bar_dict['progress']
        progress.update(task_id=progress_bar_dict['task_id'], advance=0)

        for entry in partition:
            id = entry['id']
            url = entry['url'].decode('utf-8')

            # Scrape the URL, then compare the checksums
            # of the re-scraped .html with the original .html from the auhouseprices_landing_dir
            # If the checksums are different, attempt to re-parse the content

            # Get the original .html file
            original_html_file = os.path.join(self.auhouseprices_landing_dir, f"{id}.html")
            async with self.lock:
                with open(original_html_file, 'rb') as f:
                    original_html = f.read()
                    original_checksum = hashlib.md5(original_html).hexdigest()

            # Scrape the listing
            response = await self.get_url(url, "return_response_object", use_proxy=True)
            if response:
                re_scraped_html = response
                re_scraped_checksum = hashlib.md5(re_scraped_html).hexdigest()
                async with self.lock:
                    if re_scraped_checksum != original_checksum:
                        # Overwrite the original .html file with the re-scraped .html
                        progress_bar_dict['num_overwritten'] += 1
                        with open(original_html_file, 'wb') as f:
                            f.write(re_scraped_html)
                    else:
                        progress_bar_dict['num_identical'] += 1

                # Update the progress bar
                progress.update(task_id=progress_bar_dict['task_id'], advance=1)

    async def get_all_listing_urls(self, shared_data: DictProxy):
        '''
        Get all the URLs of rental listings in VIC from the sitemap.xml file
        '''
        # If the file already exists, read the URLs from the file
        all_urls_file = os.path.join(self.auhouseprices_landing_dir, 'all_urls.txt')
        if os.path.exists(all_urls_file):
            with open(all_urls_file, 'r') as f:
                rental_listing_urls = f.readlines()
                rental_listing_urls = [url.strip() for url in rental_listing_urls]
                print(f"Total number of VIC Rental Listings: {len(rental_listing_urls)}")
                return rental_listing_urls

        # Download and parse the sitemap.xml file
        response = await self.get_url(self.sitemap_url, "return_response_object", use_proxy=False)
        sitemap_content = response

        # Parse the sitemap XML
        soup = BeautifulSoup(sitemap_content, 'lxml-xml')

        # Filter the URLs to only include the sitemap URLs for rental listings in VIC
        filtered_urls = []
        rental_listing_urls =[]
        for sitemap in soup.find_all('sitemap'):
            loc = sitemap.find('loc').text
            if 'sitemap-rent-view-VIC' in loc:
                filtered_urls.append(loc)

        total_files = len(filtered_urls)

        with Progress(
            SpinnerColumn(),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.1f}%"),
            TextColumn("{task.completed}/{task.total} sitemaps processed"),
        ) as progress:
            task = progress.add_task("Processing files...", total=total_files)

            # Download and extract the .xml.gz files
            for url in filtered_urls:
                gz_content = await self.get_url(url, "stream_to_file", use_proxy=False)
                gz_filename = url.split('/')[-1]
                xml_filename = gz_filename.replace('.gz', '')

                # Save the .gz file
                output_gz_file = os.path.join(self.tmp_dir, gz_filename)
                with open(output_gz_file, 'wb') as f:
                    f.write(gz_content)

                # Extract the .gz file
                with gzip.open(output_gz_file, 'rb') as f_in:
                    input_xml_file = os.path.join(self.tmp_dir, xml_filename)
                    with open(input_xml_file, 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)

                # Parse the extracted XML file
                tree = ET.parse(input_xml_file)
                xml_root = tree.getroot()

                # Extract the URLs from the XML file
                for url in xml_root.findall('{http://www.sitemaps.org/schemas/sitemap/0.9}url'):
                    loc_element = url.find('{http://www.sitemaps.org/schemas/sitemap/0.9}loc')
                    if loc_element is not None:
                        loc = loc_element.text
                        # Then add the URL to the list of listing URLs
                        rental_listing_urls.append(loc)
                    else:
                        loc = None

                # Clean up the downloaded and extracted files
                os.remove(output_gz_file)
                os.remove(input_xml_file)
                # Update the progress bar
                progress.update(task, advance=1)

        # Write all the URLs to a file
        output_all_urls_file = os.path.join(self.auhouseprices_landing_dir, 'all_urls.txt')
        with open(output_all_urls_file, 'w') as f:
            for url in rental_listing_urls:
                f.write(f"{url}\n")

        print(f"Total number of VIC Rental Listings: {len(rental_listing_urls)}")
        return rental_listing_urls

    async def main(self, shared_data: DictProxy):

        rental_listing_urls = await self.get_all_listing_urls(shared_data)
        rental_listing_urls = rental_listing_urls[:1000]
        shared_data["rental_listing_urls"][0] = set([get_suffix(url) for url in rental_listing_urls])
        partitions = np.array_split(np.array(rental_listing_urls, dtype=StringDType()), 400)

        # Initialize the progress bar
        progress = Progress(
            SpinnerColumn(),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.1f}%"),
            TextColumn("{task.completed}/{task.total} listings"),
        )
        progress_bar_dict = {
            'progress': progress
        }

        with progress:
            task_id = progress.add_task("Processing listings...", total=len(rental_listing_urls))
            progress_bar_dict['task_id'] = task_id

            parsed_listings = []

            tasks = []
            for partition in partitions:
                task = asyncio.create_task(
                    self.scrape_listings(partition, progress_bar_dict, shared_data)
                )
                tasks.append(task)

            await asyncio.gather(*tasks)

            print(f"Scraped {shared_data['num_scraped']} listings")


def display_progress(shared_progress):
    with Progress(
        SpinnerColumn(),
        BarColumn(),
        TextColumn("[progress.percentage]{task.percentage:>3.1f}%"),
        TextColumn("{task.completed}/{task.total} listings processed"),
    ) as progress:
        task = progress.add_task("Parsing listings...", total=shared_progress['total'])
        while shared_progress['completed'] <= shared_progress['total']:
            progress.update(task, completed=shared_progress['completed'])

        progress.update(task, completed=shared_progress['total'])

def parse_listings_wrapper(args):
    return parse_listings(*args)

def parse_listings(base_raw_dir, auhouseprices_raw_dir, auhouseprices_landing_dir,
    partition: np.ndarray, partition_idx: int, shared_data: DictProxy, shared_progress: DictProxy, address_erroneous_files=False, multiprocessing_lock=None):
    '''
    Function to parse each of the listings
    '''
    check_make_dir(base_raw_dir)
    check_make_dir(auhouseprices_raw_dir)

    erroneous_files = set()
    error_data = {
        'div.breadcrumbs-v3 error' : 0,
        'div.container.content error' : 0,
        'nested_row_error' : 0
    }

    parsed_listings = []

    # Iterate through each .html file in the landing directory
    for file in partition:
        if file.endswith(".html"):
            input_file = os.path.join(auhouseprices_landing_dir, f"{file}")
            with open(input_file, 'rb') as f:
                listing = BeautifulSoup(f, 'lxml')

                listing_info = {}

                # Extract unique ID, which is the filename without the .html extension
                unique_id = int(file.split('.')[0])
                listing_info['unique_id'] = unique_id

                # Use CSS selectors to directly find the elements
                container = listing.select_one('div.breadcrumbs-v3')
                if container is None:
                    error_data['div.breadcrumbs-v3 error'] += 1
                    erroneous_files.add(file)
                else:
                    # Extract breadcrumb information
                    breadcrumb = container.select_one('ul.breadcrumb li.active')
                    street_name = breadcrumb.text.strip() if breadcrumb else None
                    listing_info['street_name'] = street_name

                    # Extract state information
                    state = container.select_one('ul.breadcrumb li a[href*="/vic/"]')
                    state_text = state.text.strip() if state else None
                    listing_info['state'] = state_text if state_text else None

                    # Extract suburb and postcode information
                    suburb = container.select_one('ul.breadcrumb li a[href*="/rent/list/"]')
                    suburb_text = suburb.text.strip() if suburb else None
                    suburb_match = re.match(r'(\d+)\s+(.+)', suburb_text) if suburb_text else None
                    postcode = suburb_match.group(1) if suburb_match else None
                    suburb_name = suburb_match.group(2) if suburb_match else None
                    listing_info['postcode'] = postcode
                    listing_info['suburb'] = suburb_name

                container = listing.select_one('div.container.content')
                if container is None:
                    error_data['div.container.content error'] += 1
                    erroneous_files.add(file)
                    continue

                # Find the nested row
                nested_row = container.select_one('div.row > div.col-md-8.col-sm-7 > div.tag-box.tag-box-v2.box-shadow.shadow-effect-1 > div.row')
                if nested_row is None:
                    error_data['nested_row_error'] += 1
                    erroneous_files.add(file)
                    continue

                # Extract property type
                property_type_tag = nested_row.select_one('div.col-md-4 ul.list-unstyled li span.badge')
                property_type = property_type_tag.text.strip() if property_type_tag else None
                listing_info['property_type'] = property_type

                # Extract address
                address_tag = nested_row.select_one('div.col-md-8 h2')
                address = address_tag.text.strip() if address_tag else None
                listing_info['address'] = address

                # Extract rent
                rent_tag = nested_row.select_one('div.col-md-8 h5 code')
                rent = rent_tag.text.strip() if rent_tag else None
                listing_info['rent'] = float(re.sub(r'[^\d.]', '', rent)) if rent else None

                # Extract number of bedrooms, bathrooms, and parking
                num_bedrooms_tag = nested_row.select_one('div.col-md-4 ul.list-unstyled li big')
                num_bathrooms_tag = nested_row.select_one('div.col-md-4 ul.list-unstyled li i.i-bath')
                parking_tag = nested_row.select_one('div.col-md-4 ul.list-unstyled li i.i-car')

                num_bedrooms = num_bedrooms_tag.text.strip() if num_bedrooms_tag else None
                num_bathrooms = num_bathrooms_tag.next_sibling.strip() if num_bathrooms_tag else None
                parking = parking_tag.next_sibling.strip() if parking_tag else None

                listing_info['num_bedrooms'] = int(num_bedrooms) if num_bedrooms else None
                listing_info['num_bathrooms'] = int(num_bathrooms) if num_bathrooms else None
                listing_info['parking'] = int(parking) if parking else None

                # Extract distance to CBD
                distance_to_cbd_tag = nested_row.select_one('div.col-md-8 ul.list-unstyled li:-soup-contains("Distance To CBD") code')
                distance_to_cbd = distance_to_cbd_tag.text.strip() if distance_to_cbd_tag else None
                listing_info['distance_to_cbd'] = float(re.sub(r'[^\d.]', '', distance_to_cbd)) if distance_to_cbd else None

                # Extract sold date (if available)
                sold_date_tag = nested_row.select_one('div.col-md-8 h5')
                if sold_date_tag:
                    match = re.search(r'on (\w+ \d{4})', sold_date_tag.text)
                    sold_date = match.group(1) if match else None
                else:
                    sold_date = None
                listing_info['sold_date'] = sold_date

                # Extract latitude and longitude (if available)
                # Assuming latitude and longitude are available in the href attribute of the nearby links
                nearby_link = nested_row.select_one('div.col-md-8 ul.list-unstyled li a[href*="lat"]')
                if nearby_link:
                    href = nearby_link['href']
                    lat_lng_match = re.search(r'lat/([-.\d]+)/lng/([-.\d]+)', href)
                    if lat_lng_match:
                        latitude = lat_lng_match.group(1)
                        longitude = lat_lng_match.group(2)
                        listing_info['latitude'] = float(latitude) if latitude else None
                        listing_info['longitude'] = float(longitude) if longitude else None


                # Find all table rows
                rows = listing.select('table.table tr')

                num_extra_urls_found = 0
                if rows:
                    # Iterate over each row and extract the URL
                    for row in rows[1:]:  # Skip the header row
                        link = row.select_one('td a')
                        if link:
                            url = link['href']
                            suffixed_url = get_suffix(url)
                            # Check if the URL contains 'rent' and 'VIC'
                            if 'rent' in suffixed_url and 'VIC' in suffixed_url:
                                if suffixed_url not in shared_data["rental_listing_urls"][0]:
                                    shared_data["extra_urls"][0].add(suffixed_url)
                                    num_extra_urls_found += 1
                if num_extra_urls_found > 0:
                    print(f"Found {num_extra_urls_found} extra URLs in the listing page")


                parsed_listings.append(listing_info)
                if multiprocessing_lock:
                    with multiprocessing_lock:
                        shared_data["num_extra_urls"] += num_extra_urls_found
                        shared_data["num_parsed"] += 1
                        # Update the progress bar
                        shared_progress['completed'] += 1

    if parsed_listings:
        # Save the parsed listings to a CSV file
        if address_erroneous_files:
            output_file = os.path.join(auhouseprices_raw_dir, f"fixed_erroneous_listings-{partition_idx}.csv")
        else:
            output_file = os.path.join(auhouseprices_raw_dir, f"parsed_listings_partition-{partition_idx}.csv")
        if multiprocessing_lock:
            with multiprocessing_lock:
                with open(output_file, mode='w', newline='', encoding='utf-8') as csv_file:
                    fieldnames = parsed_listings[0].keys() if parsed_listings else []
                    writer = csv.DictWriter(csv_file, fieldnames=fieldnames)

                    writer.writeheader()
                    for listing in parsed_listings:
                        writer.writerow(listing)


    # print(f"div.breadcrumbs-v3 error: {error_data['div.breadcrumbs-v3 error']}")
    # print(f"div.container.content error: {error_data['div.container.content error']}")
    # print(f"nested_row_error: {error_data['nested_row_error']}")

    if erroneous_files:
        # Save erroneous URLs to a file
        if address_erroneous_files:
            output_erroneous_file = os.path.join(auhouseprices_raw_dir, f"remaining_erroneous_urls_partition-{partition_idx}.txt")
        else:
            output_erroneous_file = os.path.join(auhouseprices_raw_dir, f"erroneous_urls_partition-{partition_idx}.txt")
        with open(output_erroneous_file, 'w', encoding='utf-8') as f:
            for url in erroneous_files:
                f.write(f"{url}\n")
            print(f"\nTotal number of erroneous listings for this partition: {len(erroneous_files)}")

script_dir = os.path.dirname(os.path.abspath(__file__))
base_raw_dir = os.path.abspath(os.path.join(script_dir, '../data/raw/'))
proxy_list = []
# If 'usable_country_codes.txt' exists, read the country codes from the file
if os.path.exists(f'{base_raw_dir}/proxyscrape_proxies.txt'):
     with open(f'{base_raw_dir}/proxyscrape_proxies.txt', 'r') as file:
         proxy_list = file.read().splitlines()
