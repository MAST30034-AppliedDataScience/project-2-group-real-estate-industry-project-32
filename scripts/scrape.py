from contextlib import nullcontext
import os
import threading
import numpy as np
from urllib.parse import urlparse, parse_qs, urlencode
import requests
import base64
import json

landing_dir = '../data/landing/'
domain_dir = '../data/landing/domain/'
url_endpoint = 'https://www.domain.com.au'
base_url = 'https://www.domain.com.au/rent/'

URLSearchParams = {'ptype': 
    ['apartment-unit-flat,block-of-units,duplex,free-standing,new-apartments,new-home-designs,new-house-land,pent-house,semi-detached,studio,terrace,town-house,villa'], 
    'excludedeposittaken': ['0'], 
    'state': ['vic']
}
nested_keys_for_state = ["digitalData", "page", "pageInfo", "property", "state"]
nested_keys_for_mode = ["digitalData", "pageFromGraph", "category", "primaryCategory"]

def key_in(data, key):
    '''
    Check if the key exists in the dictionary
    '''
    if not isinstance(data, dict):
        return False
    return key in data  

def get_nested_value(d, nested_keys_for_state):
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
    
def get_property(data, visited_urls: set, collided_urls: set, shared_data: dict, lock: threading.Lock):
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
            print("No listings map found")
            continue
        
        listings_map = current_data["props"]["listingsMap"]
        
        for listing in listings_map:        
            if not key_in(listings_map[listing], "listingModel"):
                print("No listingModel found")
                continue
            if not key_in(listings_map[listing]["listingModel"], "url"):
                print("No url found in listingModel")
                continue
            if not key_in(listings_map[listing], "id"):
                print("No id found")
                continue
            id = listings_map[listing]["id"]              
            
            try:
                # Check if the URL is already a full URL
                listing_url = listings_map[listing]["listingModel"]["url"]
                if not listing_url.startswith("http"):
                    url = f'{url_endpoint}/{listing_url}'
                else:
                    url = listing_url
                    
                with lock:
                    if url in visited_urls:
                        len_before = len(collided_urls)
                        collided_urls.add(url)
                        if len_before != len(collided_urls) and len(collided_urls) % 200 == 0:
                            print(f'Already seen {len(collided_urls)} URLs')
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
                        print("No property data found")
                        continue
                
                if not key_in(new_data["props"], "stateAbbreviation"):
                    print("No stateAbbreviation found")
                    continue
                    
                state = new_data["props"]["stateAbbreviation"]
                
                if state is None:
                    state = get_nested_value(new_data, nested_keys_for_state)
                    
                    if state:
                        new_data["props"]["stateAbbreviation"] = state
                        print("Replacing stateAbbreviation with state found in digitalData")
                    else:
                        # Determine which key is missing and print the corresponding message
                        for i, key in enumerate(nested_keys_for_state):
                            if get_nested_value(new_data, nested_keys_for_state[:i+1]) is None:
                                print(f"No {key} found")
                                break
                        else:
                            print("This state is also None")
                        continue
                    
                if state.lower() != "vic":                    
                    continue
                    
                listing_summary = new_data["props"]["listingSummary"]            
                if (
                    not key_in(listing_summary, "listingType") or 
                    not key_in(listing_summary, "mode")
                ):
                    print("No listingType or mode found")
                    continue
                    
                mode = listing_summary["mode"]
                if (listing_summary["mode"] != "rent"):
                    mode = get_nested_value(new_data, nested_keys_for_state)                        
                    if mode and mode == "rent":
                        new_data["props"]["listingSummary"]["mode"] = mode.lower()
                        print("Replacing mode with mode found in digitalData")
                    else:
                        # Determine which key is missing and print the corresponding message
                        for i, key in enumerate(nested_keys_for_state):
                            if get_nested_value(new_data, nested_keys_for_state[:i+1]) is None:
                                print(f"No {key} found, replaced mode is None")
                                break
                        continue
                    
                    
                stack.append(new_data)
                
                # Use the lock to ensure that only one thread is accessing the directory at a time.
                with lock:                
                    # Check if .json file already exists
                    if os.path.exists(f'{domain_dir}{id}.json'):
                        shared_data["num_file_already_seen"] += 1                    
                        if shared_data["num_file_already_seen"] % 200 == 0:
                            print(f'Already seen {shared_data["num_file_already_seen"]} listings')
                        continue  
                    
                    # Write the listing to the landing directory            
                    with open(f'{domain_dir}{id}.json', 'w') as f:
                        json.dump(new_data, f)  
                        shared_data["num_written"] += 1 
                        if shared_data["num_written"] % 200 == 0:
                            print(f'Scraped {shared_data["num_written"]} listings')     
                    
            except requests.exceptions.RequestException as err:
                print(err.response)
                continue

           
def get_rental_listings(pages, visited_urls: set, collided_urls: set, shared_data: dict, lock: threading.Lock):
    '''
    Get the listings from the domain.com.au website
    '''           
    # Convert the dictionary to a URL-encoded query string
    url = (
        f'{base_url}?ptype={URLSearchParams["ptype"][0]}'
        f'&excludedeposittaken={URLSearchParams["excludedeposittaken"][0]}'
        f'&state={URLSearchParams["state"][0]}'
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
            get_property(data, visited_urls, collided_urls, shared_data, lock)
            print(f'Finished page {i}')            
        except requests.exceptions.RequestException as err:
            print(f'get_rental_listings: {err.response}')
            return None   
    
     


# We are going to use threading to speed up the process
lock = threading.Lock()
pages = list(range(1, 51))
partitions = np.array_split(pages, 13)

if __name__ == "__main__":
    shared_data = {
        'num_file_already_seen': 0,
        'num_written': 0
    }
    
    if not os.path.exists(landing_dir):
        os.mkdir(landing_dir) 
    if not os.path.exists(f'{domain_dir}'):
        os.mkdir(f'{domain_dir}')
           
    visited_urls = set()
    collided_urls = set()
    threads = []
    
    for partition in partitions:
        thread = threading.Thread(target=get_rental_listings, args=(partition, visited_urls, collided_urls, shared_data, lock))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()
    
    print(f'Finished scraping {shared_data["num_written"]} listings')  
