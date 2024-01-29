# Qumulo File System Event Monitor

This project uses the Qumulo Change Notify API to monitor for changes to a watched portion of the file system and triggers the launching of modules in response to specific conditions.

Conversations with Scott U. indicate that it is likely less impactful to simply monitor changes to `/` than to launch multiple watchers.

The workflow would consist of:

- `cn_monitor.py` which initiates a Change Notify API subscription and listens for events

- A Redis queue to hold events. 

- `event_filter.py` which subscribes to the queue filters events based on a user-supplied criteria stored in a config file (Maybe bundle cn_monitor and event_filter as one script?).  

- `watched_items.conf`  A file containing a list of watched directories or files and the actions to take when matched

- `action_modules.conf` A file containing a list of modules to perform specific actions based on trigger events.  This could also be a directory

- A Docker or Kubernetes deployment to run all of the above.

## Hypothetical sample workflow:

1. `cn_monitor.py` monitors all recursive changes to a cluster's root directory
2. Changes are pushed into a queue (Redis, most likely)
3. `event_filter.py` subscribes to the Redis queue and filters for matches in `watched_items.conf`
4. Matches then trigger the appropriate Action Module.

Notes:

The Event Filter should ideally have AND/OR condition matching logic and should also allow the application of changes to the `watched_items.conf` file without stopping `cn_monitor.py` or losing events.

## Sample Action Modules:

- Quarantine specific file types (.jpg, .zip, .mp4, etc)
- Trigger `all_stop.py` if a set of watched files are modified
- Rename files in a watched directory
- Change permissions on files in a watched directory
- Send an email or message if a file or directory has been changed or created (Integrate with Qumulo Email Alerts?)

