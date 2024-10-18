from urllib.parse import urlparse, parse_qs, urlencode
from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn
from rich.table import Table
from contextlib import nullcontext
from auhouseprices_scraper import AuHousePricesScraper, parse_listings_wrapper, display_progress
import multiprocessing
from multiprocessing import Manager, Pool
from multiprocessing.managers import DictProxy
import numpy as np
from numpy.dtypes import StringDType
import os
import threading
import requests
import base64
import json

def key_in(data, key):
    '''
    Check if the key exists in the dictionary
    '''
    if not isinstance(data, dict):
        return False
    return key in data

class DomainScraper:
    '''
    Class to scrape the domain.com.au website for Victorian Rental listings
    '''
    def __init__(self) -> None:
        self.script_dir = os.path.dirname(os.path.abspath(__file__))
        self.landing_dir = os.path.abspath(os.path.join(self.script_dir, '../data/landing/'))
        self.domain_dir = os.path.abspath(os.path.join(self.script_dir, "../data/landing/domain/"))
        self.url_endpoint = 'https://www.domain.com.au'
        self.base_url = 'https://www.domain.com.au/rent/'
        self.URLSearchParams = {'ptype': 
            ['apartment-unit-flat,block-of-units,duplex,free-standing,new-apartments,new-home-designs,new-house-land,pent-house,semi-detached,studio,terrace,town-house,villa'], 
            'excludedeposittaken': ['0'], 
            'state': ['vic']
        }
        self.nested_keys_for_state = ["digitalData", "page", "pageInfo", "property", "state"]
        self.nested_keys_for_mode = ["digitalData", "pageFromGraph", "category", "primaryCategory"] 
        
        
        # We are going to use threading to speed up the process
        self.lock = threading.Lock()
        self.pages = list(range(1, 51))
    
    
    def start(self):
        partitions = np.array_split(self.pages, 8)
        self.shared_data = {
            'num_file_already_seen': 0,
            'num_written': 0
        }
        
        if not os.path.exists(self.landing_dir):
            os.mkdir(self.landing_dir) 
        if not os.path.exists(f'{self.domain_dir}'):
            os.mkdir(f'{self.domain_dir}')
                
        visited_urls = set()
        collided_urls = set()
        threads = []
        
        with Progress(
            SpinnerColumn(),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.1f}%"),
            TextColumn("{task.completed}/{task.total} pages completed"),
        ) as progress:
            task = progress.add_task("Processing pages...", total=50)
        
            for partition in partitions:
                thread = threading.Thread(target=self.get_rental_listings, 
                    args=(partition, visited_urls, collided_urls, self.shared_data, self.lock, progress, task))
                threads.append(thread)
                thread.start()
        
            for thread in threads:
                thread.join()
        
        print(f'Already previously scraped {self.shared_data["num_file_already_seen"]} listings')
        print(f'Collided with {len(collided_urls)} URLs during search')        
        print(f'Finished scraping {self.shared_data["num_written"]} listings')   
      
    
    def get_nested_value(self, d, nested_keys_for_state):
        """
        Safely get a nested value from a dictionary.
        
        :param d: The dictionary to search.
        :param nested_keys_for_state: A list of nested_keys_for_state to navigate through the dictionary.
        :return: The value if found, otherwise None.
        """
        for key in nested_keys_for_state:
            if isinstance(d, dict) and key in d:
                d = d[key]
            else:
                return None
        return d

    
    def get_property(self, data, visited_urls: set, collided_urls: set, shared_data: dict, lock: threading.Lock):
        '''
        Get the property details from the listing map
        '''    
        headers = {
            'accept': 'application/json',
            'Content-Type': 'application/json',
            'User-Agent' : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36 Edg/128.0.0.0'
        }
        
        stack = [data]   
        
        while stack:           
            current_data = stack.pop()
            
            if ("props" not in current_data) or ("listingsMap" not in current_data["props"]) or (current_data["props"]["listingsMap"] is None):
                # print("No listings map found")
                continue
            
            listings_map = current_data["props"]["listingsMap"]
            
            for listing in listings_map:        
                if not key_in(listings_map[listing], "listingModel"):
                    # print("No listingModel found")
                    continue
                if not key_in(listings_map[listing]["listingModel"], "url"):
                    # print("No url found in listingModel")
                    continue
                if not key_in(listings_map[listing], "id"):
                    # print("No id found")
                    continue
                id = listings_map[listing]["id"]              
                
                try:
                    # Check if the URL is already a full URL
                    listing_url = listings_map[listing]["listingModel"]["url"]
                    if not listing_url.startswith("http"):
                        url = f'{self.url_endpoint}/{listing_url}'
                    else:
                        url = listing_url
                        
                    with self.lock:
                        if url in visited_urls:
                            len_before = len(collided_urls)
                            collided_urls.add(url)                                
                            continue
                        
                        visited_urls.add(url)
                        
                    response = requests.get(url, headers=headers)
                    response.raise_for_status()
                    # Load the JSON data into a dictionary
                    new_data = response.json()
                    
                    if not key_in(new_data, "props") or not key_in(new_data["props"], "listingSummary"):
                        if (
                            not key_in(new_data, "digitalData") or 
                            not key_in(new_data["digitalData"], "page") or
                            not key_in(new_data["digitalData"]["page"], "pageInfo") or
                            not key_in(new_data["props"], "rootGraphQuery") or
                            not key_in(new_data["props"]["rootGraphQuery"], "listingByIdV2")
                        ):
                            # print("No property data found")
                            continue
                    
                    if not key_in(new_data["props"], "stateAbbreviation"):
                        # print("No stateAbbreviation found")
                        continue
                        
                    state = new_data["props"]["stateAbbreviation"]
                    
                    if state is None:
                        state = self.get_nested_value(new_data, self.nested_keys_for_state)
                        
                        if state:
                            new_data["props"]["stateAbbreviation"] = state
                            # print("Replacing stateAbbreviation with state found in digitalData")
                        else:
                            # Determine which key is missing and print the corresponding message
                            for i, key in enumerate(self.nested_keys_for_state):
                                if self.get_nested_value(new_data, self.nested_keys_for_state[:i+1]) is None:
                                    # print(f"No {key} found")
                                    break
                            # else:
                            #     print("This state is also None")                            
                            continue
                        
                    if state.lower() != "vic":                    
                        continue
                        
                    listing_summary = new_data["props"]["listingSummary"]            
                    if (
                        not key_in(listing_summary, "listingType") or 
                        not key_in(listing_summary, "mode")
                    ):
                        # print("No listingType or mode found")
                        continue
                        
                    mode = listing_summary["mode"]
                    if (listing_summary["mode"] != "rent"):
                        mode = self.get_nested_value(new_data, self.nested_keys_for_state)                        
                        if mode and mode == "rent":
                            new_data["props"]["listingSummary"]["mode"] = mode.lower()
                            # print("Replacing mode with mode found in digitalData")
                        else:
                            # Determine which key is missing and print the corresponding message
                            for i, key in enumerate(self.nested_keys_for_state):
                                if self.get_nested_value(new_data, self.nested_keys_for_state[:i+1]) is None:
                                    # print(f"No {key} found, replaced mode is None")
                                    break
                            continue
                        
                        
                    stack.append(new_data)
                    
                    # Use the lock to ensure that only one thread is accessing the directory at a time.
                    with self.lock:                
                        # Check if .json file already exists
                        candidate_file = os.path.join(self.domain_dir, f'{id}.json')
                        if os.path.exists(candidate_file):
                            self.shared_data["num_file_already_seen"] += 1
                            continue  
                        
                        # Write the listing to the landing directory            
                        with open(candidate_file, 'w') as f:
                            json.dump(new_data, f)  
                            self.shared_data["num_written"] += 1   
                        
                except requests.exceptions.RequestException as err:
                    print(err.response)
                    continue

           
    def get_rental_listings(self, pages, visited_urls: set, collided_urls: set, shared_data: dict, lock: threading.Lock, progress, task):
        '''
        Get the listings from the domain.com.au website
        '''           
        # Convert the dictionary to a URL-encoded query string
        url = (
            f'{self.base_url}?ptype={self.URLSearchParams["ptype"][0]}'
            f'&excludedeposittaken={self.URLSearchParams["excludedeposittaken"][0]}'
            f'&state={self.URLSearchParams["state"][0]}'
        )
        
        headers = {
            'accept': 'application/json',
            'Content-Type': 'application/json',
            'User-Agent' : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36 Edg/128.0.0.0'
        }       
        # Iterate through the pages, the maximum number of pages seems to be limited to 50
        for i in pages:
            try:                            
                url = f'{url}&page={i}'
                response = requests.get(url, headers=headers)
                response.raise_for_status()
                # Load the JSON data into a dictionary
                data = response.json()
                self.get_property(data, visited_urls, collided_urls, self.shared_data, self.lock)                                
            except requests.exceptions.RequestException as err:
                print(f'get_rental_listings: {err.response}')
                return None          
            
            # Update the progress bar
            progress.update(task, advance=1)

if __name__ == "__main__":  
    print("Follow each step in order")  
    print("\t[1] Scrape domain.com.au rental listings")
    print("\t[2] Scrape auhouseprices.com historical rental listings")
    print("\t[3] Phase 1: Parse auhouseprices.com scraped data into the raw directory")
    print("\t[4] Phase 2: Parse auhouseprices.com scraped data into the raw directory")
    print("\t[5] Scrape CoreLogic Public API property data")    
    choice = int(input("Select: "))
    
    match choice:
        case 1:
            domain_scraper = DomainScraper()
            domain_scraper.start()
        case 2:
            auhouseprices_scraper = AuHousePricesScraper(phase=1)
            auhouseprices_scraper.start()
        case _:
            auhouseprices_scraper = AuHousePricesScraper(phase=choice-1)
            landing_dir_files = auhouseprices_scraper.start()

            manager = Manager()
            shared_data = manager.dict()
            shared_data['num_file_already_seen'] = 0
            shared_data['num_scraped'] = 0
            shared_data['num_parsed'] = 0
            shared_data['num_extra_urls'] = 0
            shared_data['extra_urls'] = manager.list()
            shared_data['extra_urls'].append(set())
            shared_data['rental_listing_urls'] = manager.list()
            shared_data['rental_listing_urls'].append(set())
            shared_progress = manager.dict()
            shared_progress['completed'] = 0
            shared_progress['total'] = len(landing_dir_files)

            multiprocessing_lock = manager.Lock()

            partitions = np.array_split(np.array(landing_dir_files, dtype=StringDType()), 8)
            
            progress_process = multiprocessing.Process(target=display_progress, args=(shared_progress,))
            progress_process.start()

            pool = Pool(processes=8)

            pool.map(parse_listings_wrapper,
                [(auhouseprices_scraper.base_raw_dir,
                    auhouseprices_scraper.auhouseprices_raw_dir,
                    auhouseprices_scraper.auhouseprices_landing_dir,
                    partitions[partition_idx],
                    partition_idx,
                    shared_data,
                    shared_progress,
                    auhouseprices_scraper.address_erroneous_files,
                    multiprocessing_lock) for partition_idx in range(len(partitions))])

            pool.close()
            pool.join()

            print(f"\nTotal number of listings parsed: {shared_data['num_parsed']}")
            progress_process.terminate()

            manager.shutdown()