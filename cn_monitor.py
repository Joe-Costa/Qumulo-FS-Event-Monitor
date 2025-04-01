#!/usr/bin/env python3
import urllib3
import configparser
import asyncio
import aiohttp
import json
import os
import requests
import json
from aiohttp import web



# Load the config file
config = configparser.ConfigParser()
config.read("cn_monitor.conf")
CLUSTER_ADDRESS = config["CLUSTER"]["CLUSTER_ADDRESS"]
TOKEN = config["CLUSTER"]["TOKEN"]
USE_SSL = config["CLUSTER"].getboolean('USE_SSL')
WATCHED_FOLDERS = ['home/joe/hooks', '/JuanUlloa']
WEBHOOK_URL = 'https://hooks.slack.com/services/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'
WEBHOOK_HEADERS = {
    'Content-type': 'application/json',
    }
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


async def webhook_handler(request):
    data = await request.json()  # Assuming the incoming data is JSON encoded
    new_path = data.get('text')  # Adjust 'text' to the actual key depending on how the data is sent
    if new_path:
        WATCHED_FOLDERS.append(new_path)
        return web.Response(text=f"Added {new_path} to watched folders", status=200)
    return web.Response(text="Invalid data", status=400)

async def start_web_server():
    app = web.Application()
    app.add_routes([web.post('/webhook', webhook_handler)])  # Listening on /webhook endpoint
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, 'localhost', 5000)  # Listen on localhost:8080, adjust as necessary
    await site.start()
    print("Web server started. Listening for incoming webhooks...")


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
            print(fs_event['path'])
            # if fs_event['type'] == 'child_file_added':
            #     # print(f"New file created: {os.path.basename(fs_event['path'])}")
            #     print(f"New file created: {fs_event['path']}")

            # else:
            #     pass
            # if WATCHED_FOLDERS in fs_event['path']:
            if any(folder in '/' + fs_event['path'] for folder in WATCHED_FOLDERS):
                event_folder = os.path.dirname(fs_event['path'])
                # print(f"File {os.path.basename(fs_event['path'])} created in {WATCHED_FOLDER}")
                # print(f"New file created in : {fs_event['path']}")
                # if fs_event['type'] == 'child_dir_added':
                #     file_id = fs_event['spine'][-1]
                path = os.path.basename(fs_event['path'])
                if fs_event['type'] == 'child_file_removed' or fs_event['type'] == 'child_dir_removed':
                    event = json.dumps({"text": f"Object {path} deleted from {event_folder}"})
                    response = requests.post(WEBHOOK_URL, headers=WEBHOOK_HEADERS, data=event)
                elif fs_event['type'] == 'child_file_added' or fs_event['type'] == 'child_dir_added':
                    event = json.dumps({"text": f"Object {path} created in {event_folder}"})
                    response = requests.post(WEBHOOK_URL, headers=WEBHOOK_HEADERS, data=event)

                else:
                    pass
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
# async def main():
#     webhook_server = start_web_server()
#     api_monitor = monitor_api()
#     await asyncio.gather(webhook_server, api_monitor)  # Run both the webhook listener and API monitor concurrently


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

