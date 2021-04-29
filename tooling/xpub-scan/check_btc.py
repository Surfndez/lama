import requests
import json
import os
import subprocess
import sys
from time import sleep
import argparse

account_endpoint = '/accounts'

# command line args (--xpub, --scheme, [--base-url])
parser = argparse.ArgumentParser()
parser.add_argument('--base-url', 
                    help = "LaMa's base URL")

parser.add_argument('--xpub', 
                    required = True,
                    help = 'xpub to analyze')

parser.add_argument('--scheme', 
                    required = True,
                    help = 'scheme associated with the xpub')

args = parser.parse_args()

def get_base_url(base_url, local_mac_os = False):
    # provided base url or localhost
    base_url = base_url if base_url is not None else 'http://localhost:8080'

    # a specific url has to be used for localhost on macOS
    if local_mac_os:
        base_url = 'http://docker.for.mac.host.internal:8080'

    try:
        requests.get(f"{base_url}/accounts")
    except requests.exceptions.ConnectionError:
        if not local_mac_os:
            return get_base_url(base_url, local_mac_os = True)
        else:
            print(f"Error: LaMa is not available at {base_url}")
            exit(1)

    print(f"Base url: {base_url}")
    return base_url

base_url = get_base_url(args.base_url)
xpub = args.xpub
scheme = args.scheme.upper()

# fetch operations from LaMa
def get_operations(limit = 100, next_cursor = None, operations = None):
    if next_cursor is None:
        url = f"{base_url}{account_endpoint}/{account_id}/operations?limit={limit}"
    else:
        url = f"{base_url}{account_endpoint}/{account_id}/operations?cursor={next_cursor}&limit={limit}"

    print(f"Querying {url}...")

    r = requests.get(url)

    ops = json.loads(r.text)

    if ops['cursor'] is None:
        return ops

    next_cursor = ops['cursor']['next']

    if operations is None:
        operations = ops
    else:
        for op in ops['operations']:
            operations['operations'].append(op)

    if next_cursor is not None:
        return get_operations(limit, next_cursor, operations)
    else:
        return operations

payload = {
    "account_key": {
        "extended_public_key" : xpub
    },
    "scheme": scheme,
    "lookahead_size": 20,
    "coin": "btc",
    "sync_frequency": 60,
    "group": "Demo"
}

# register xpub
r = requests.post(f"{base_url}{account_endpoint}", json=payload)
account = json.loads(r.text)

account_id = account['account_id']
print(f"Account id: {account_id}")

# polling
print('Waiting for LaMa to synchronize the operations...')
expected_statuses = ['registered', 'published', 'synchronized']
past_statuses = set()

while True:
    r = requests.get(f"{base_url}{account_endpoint}/{account_id}")

    status = json.loads(r.text)['last_sync_event']['status']

    # ensure that the status is an expected one
    if status not in expected_statuses:
        print(f"Synchronization error: {status}")
        exit(1)

    # display new statuses only once
    for expected in expected_statuses:
        if status == expected and status not in past_statuses:
            print(f"\t→ {status}")
            past_statuses.add(status)

    # synchronized? done
    if status == 'synchronized':
        break

    sleep(.5)


# get balance
r = requests.get(f"{base_url}{account_endpoint}/{account_id}")
account = json.loads(r.text)

balance = account['balance']
print(f"Balance: {balance}")

# fetch and save LaMa JSON
operations_history = get_operations(limit = 20)

with open(f"{xpub}.json", 'w', encoding='utf-8') as f:
    json.dump(operations_history, f, ensure_ascii=False, indent=4)

# os.system(f"cat {xpub}.json")

# Xpub Scan Analysis
print('\nRunning Xpub Scan analysis')

cmd = f"node build/scan.js {xpub} --quiet --import {xpub}.json --save report --balance {balance} --diff"

exit_code = subprocess.call(cmd.split())

print(f"Exit code: {exit_code}")

sys.exit(exit_code)
