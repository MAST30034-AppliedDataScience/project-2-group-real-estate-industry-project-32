import requests
from bs4 import BeautifulSoup
import json
import html
import os
import regex as re

onthehouse_dir = "../data/landing/onthehouse"

url = "https://www.onthehouse.com.au/suburb/vic/altona-3018"

headers = {
    "Accept": "application/json",
    "User-Agent": "curl/8.8.0"
}

if not os.path.exists(onthehouse_dir):
    os.mkdir(onthehouse_dir)

response = requests.get(url, headers=headers)

# Print the status code
print(f"Status Code: {response.status_code}")

# Check the status code
if response.status_code == 200:   
    # Parse the HTML content
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Find the <script> tag containing 'window.REDUX_DATA'
    script_tag = soup.find('script', text=re.compile(r'window\.REDUX_DATA\s*=\s*{'))    
    if script_tag:
        # Extract the JSON data from the <script> tag
        script_content = script_tag.string
        
        # Use regex to extract the JSON part
        json_data_match = re.search(r'window\.REDUX_DATA\s*=\s*({.*?});', script_content, re.DOTALL)
        
        if json_data_match:
            json_data = json_data_match.group(1)            
            try:
                # Parse the JSON data
                data = json.loads(json_data)
                # Write the JSON data to a file
                with open(f"{onthehouse_dir}/data.json", "w") as file:
                    json.dump(data, file, indent=4)
            except json.JSONDecodeError as e:
                print(f"Failed to parse JSON data: {e}")
        else:
            print("JSON data not found in the script content")
    else:
        print("Script tag containing 'window.REDUX_DATA' not found in the HTML")
else:
    print(f"Request failed with status code {response.status_code}")
    # Handle non-200 status codes
    if response.status_code == 404:
        print("Error 404: Page not found")
    elif response.status_code == 500:
        print("Error 500: Internal server error")
    else:
        print("An error occurred while making the request")

    
    