#!/usr/bin/env python3
import argparse
import urllib3
import configparser
import asyncio
import aiohttp
import json
import os
import yaml


# Load the config file
config = configparser.ConfigParser()
config.read("cn_monitor.conf")
CLUSTER_ADDRESS = config["CLUSTER"]["CLUSTER_ADDRESS"]
TOKEN = config["CLUSTER"]["TOKEN"]
USE_SSL = config["CLUSTER"].getboolean('USE_SSL')

# Load the watched items from the config file
with open('watched_items.yml', 'r') as file:
    watched_items = yaml.safe_load(file)

# Get the PATHS and EXTENSIONS as lists
WATCHED_PATHS = watched_items.get('PATHS', [])
WATCHED_EXTENSIONS = watched_items.get('EXTENSIONS', [])

# Reusable headers for API calls
HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Accept": "application/json",
    "Content-Type": "application/json",
}

API_URL = "https://" + CLUSTER_ADDRESS + "/api/v1/files/{}"
# Notify on all events:
REF_PATH = "%2F/notify?recursive=true"
API_ENDPOINT = API_URL.format(REF_PATH)

'''
Sample of filtering and notify on "child_dir_added" and "child_file_added" only:

REF_PATH = "%2F/notify?filter=child_dir_added%2Cchild_file_added&recursive=true"

'''

# Do something with the data received from the CN watcher
async def handle_event(event_data):
    try:
        changes = json.loads(event_data)

        ''' 

        The sample loop below looks for new files being created and prints their names.

        An example complete, unfiltered line example would be:

        {'type': 'child_file_added', 'spine': ['2', '10003', '5007655', '1459590167'], 'path': 'home/joe/fff2', 'stream_name': None}

        Note that "changes" could contain multiple such lines per client action driven event

        The goal is to send each event line (Or maybe the entire "changes" blob...) to RabbitMQ or Redis

        '''

        for fs_event in changes:
            # Extract the file extension
            full_path = '/' + fs_event['path']
            file_extension = os.path.splitext(fs_event['path'])[1]
            file_directory = os.path.dirname(full_path) 
            
            # Check if it's a new file created with an extension in WATCHED_EXTENSIONS
            # and the directory is in WATCHED_PATHS
            if fs_event['type'] == 'child_file_added' and file_extension in WATCHED_EXTENSIONS and any (file_directory.startswith(path) for path in WATCHED_PATHS):
                print(f"New file created: {os.path.basename(fs_event['path'])}")
            else:
                pass

    # Catch occasions where non-JSON events are sent by the cluster
    except json.JSONDecodeError:
        print(f"Non-JSON event received: {event_data}")

# Monitor the CN API for changes
async def monitor_api():
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(API_ENDPOINT, headers=HEADERS, ssl=USE_SSL) as response:
                while True:
                    line = await response.content.readline()
                    if not line:
                        break  # End of stream
                    event_data = line.decode(encoding='UTF-8').strip()
                    if event_data.startswith("data:"):
                        event_data = event_data.replace("data:", "").strip()
                        await handle_event(event_data)
    except asyncio.CancelledError:
        print("Monitoring canceled. Cleaning up...")

async def main():
    while True:
        try:
            await asyncio.gather(monitor_api())
        except KeyboardInterrupt:
            print("Quitting...")
            break

if __name__ == "__main__":
    if not USE_SSL:
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    asyncio.run(main())
