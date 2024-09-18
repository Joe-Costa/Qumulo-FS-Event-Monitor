#!/usr/bin/env python3
import urllib3
import urllib
import configparser
import asyncio
import aiohttp
import uvloop
import json
from transitions import Machine, MachineError
from datetime import datetime, timedelta

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

API_ENDPOINT = API_URL.format(REF_PATH)
WATCHED_FOLDERS = ['Engineering/dropbox', 'Manufaturing/dropbox']  # List of watched folders

class FileOperation:
    states = ['waiting', 'file_moved_from', 'file_moved_to', 'completed']

    def __init__(self):
        self.machine = Machine(model=self, states=FileOperation.states, initial='waiting')
        self.machine.add_transition(trigger='file_moved_from_event', source='waiting', dest='file_moved_from')
        self.machine.add_transition(trigger='file_moved_to_event', source='file_moved_from', dest='completed')
        self.machine.add_transition(trigger='reset', source='completed', dest='waiting')

    def process_event(self, event):
        event_type = event['type']
        try:
            if event_type in ['child_file_moved_from', 'child_dir_moved_from'] and self.state == 'waiting':
                self.file_moved_from_event()
            elif event_type in ['child_file_moved_to', 'child_dir_moved_to'] and self.state == 'file_moved_from':
                self.file_moved_to_event()

            # Print current state for troubleshooting purposes
            # print(f"Current state for {event['path']}: {self.state}")

            if self.state == 'completed':
                return True  # Indicate completion
        except MachineError as e:
            print(f"State transition error: {e}")
        return False

# Dictionary to store state machines for each file or directory
file_operations = {}
move_events = {}

def get_file_key(event):
    return f"{'_'.join(event['spine'])}"

# File locker function
async def lock_file(path):
    encoded_path = urllib.parse.quote(path, safe='')
    lock_url = "https://" + CLUSTER_ADDRESS + f"/api/v1/files/%2F{encoded_path}/file-lock"
    retention_period = ( datetime.utcnow() + timedelta(seconds=60) ).strftime('%Y-%m-%dT%H:%M:%SZ')
    
    payload = {
        "retention_period": str(retention_period),
        "legal_hold": False
    }

    async with aiohttp.ClientSession() as session:
        async with session.patch(lock_url, headers=HEADERS, json=payload, ssl=USE_SSL) as response:
            if response.status == 200:
                print(f"File {path} has been locked successfully until {retention_period} (Time now is {datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')})")
            else:
                print(f"Failed to lock file {path}. Status code: {response.status}")
                response_text = await response.text()
                print(f"Response: {response_text}")
    # print(f"File {lock_url} has been locked successfully.")

# Function to delay locking the file
async def lock_file_after_delay(path, delay):
    try:
        await asyncio.sleep(delay)
        await lock_file(path)
    except Exception as e:
        print(f"Error locking file {path} after delay: {e}")

# Output alerts based on event or run file locker
async def alert_user(event_type, path, old_path=None):
    match event_type:
        case "child_file_added":
            print(f'New file has been added: {path}')
            # Schedule the lock_file function to run after n seconds
            asyncio.create_task(lock_file_after_delay(path, 5))
        case "child_dir_added":
            print(f'New directory has been added: {path}')
        case "child_file_moved_to":
            print(f'File renamed from {old_path} to {path}')
        case "child_dir_moved_to":
            print(f'Directory renamed from {old_path} to {path}')
        case _:
            print(f'Unknown event type: {event_type} for path: {path}')

# Do something with the data received from the Change Notify watcher
async def handle_event(event_data):
    try:
        changes = json.loads(event_data)

        for fs_event in changes:
            if any(fs_event['path'] == folder or fs_event['path'].startswith(folder) for folder in WATCHED_FOLDERS):
                # print(fs_event)
                event_type = fs_event['type']
                file_key = get_file_key(fs_event)
            
                if event_type in ['child_file_added', 'child_dir_added']:
                    await alert_user(event_type, fs_event['path'])  # Await alert_user
                elif event_type in ['child_file_moved_from', 'child_dir_moved_from']:
                    # print(f"Detected move from event: {fs_event['path']}")
                    if file_key not in file_operations:
                        file_operations[file_key] = FileOperation()
                        move_events[file_key] = {'from': fs_event['path'], 'to': None}
                    file_operations[file_key].process_event(fs_event)
                elif event_type in ['child_file_moved_to', 'child_dir_moved_to'] and file_key in file_operations:
                    # print(f"Detected move to event: {fs_event['path']}")
                    move_events[file_key]['to'] = fs_event['path']
                    if file_operations[file_key].process_event(fs_event):
                        await alert_user(event_type, move_events[file_key]['to'], move_events[file_key]['from'])  # Await alert_user
                        del file_operations[file_key]
                        del move_events[file_key]
            else:
                pass
    except json.JSONDecodeError:
        print(f"Non-JSON event received: {event_data}")
    except Exception as e:
        print(f"Error handling event: {e}")

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
    except Exception as e:
        print(f"Error in monitor_api: {e}")

async def main():
    while True:
        try:
            await asyncio.gather(monitor_api())
        except KeyboardInterrupt:
            print("Quitting...")
            break
        except Exception as e:
            print(f"Error in main: {e}")

if __name__ == "__main__":
    if not USE_SSL:
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    loop = asyncio.get_event_loop()

    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        pass
    finally:
        loop.close()
