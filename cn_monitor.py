#!/usr/bin/env python3
import argparse
import urllib3
import configparser
import asyncio
import aiohttp
import json
import os


# Load the config file
config = configparser.ConfigParser()
config.read("cn_monitor.conf")
CLUSTER_ADDRESS = config["CLUSTER"]["CLUSTER_ADDRESS"]
TOKEN = config["CLUSTER"]["TOKEN"]
USE_SSL = config["CLUSTER"].getboolean('USE_SSL')


HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Accept": "application/json",
    "Content-Type": "application/json",
}

API_URL = "https://" + CLUSTER_ADDRESS + "/api/v1/files/{}"

# Notify on all events:
REF_PATH = "%2F/notify?recursive=true"

# Sample of filtering and notify on "child_dir_added" and "child_file_added" only:
# REF_PATH = "%2F/notify?filter=child_dir_added%2Cchild_file_added&recursive=true"

API_ENDPOINT = API_URL.format(REF_PATH)

# Do something with the data received from the CN watcher
async def handle_event(event_data):
    try:
        changes = json.loads(event_data)

        ''' 

        The sample loop below looks for new files being created and prints their names.

        A complete, unfiltered line example would be:

        {'type': 'child_file_added', 'spine': ['2', '10003', '5007655', '1459590167'], 'path': 'home/joe/fff2', 'stream_name': None}

        Note that "changes" could contain multiple such lines per client action driven event

        The goal is to send each event line (Or maybe the entire "changes" blob...) to RabbitMQ or Redis

        '''

        for fs_event in changes:
            if fs_event['type'] == 'child_file_added':
                print(f"New file created: {os.path.basename(fs_event['path'])}")
            else:
                pass
    # Catch occasions were non-JSON events are sent by the cluster
    except json.JSONDecodeError:
        print(f"Non-JSON event received: {event_data}")

# Monitor the CN API for changes
async def monitor_api():
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(API_ENDPOINT, headers=HEADERS, ssl=USE_SSL) as response:
                while True:
                    line = await response.content.readline()
                    #print(f"LINE: {line}")
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

    loop = asyncio.get_event_loop()

    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        pass
    finally:
        loop.close()

