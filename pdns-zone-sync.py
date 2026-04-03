#!/usr/bin/env python3
# /usr/local/bin/pdns-zone-sync.py

import requests
import json
import time
import logging
import subprocess
import os
import sys

# ============ Conf ============
PRIMARY_API    = "http://127.0.0.1:port" # use script on Primary Server
PRIMARY_KEY    = "Your_API_Key" #Primary PowerDNS Authoritative Server API KEY

SECONDARY_API  = "http://host:port"
SECONDARY_KEY  = "Your_API_Key" #Secondary PowerDNS Authoritative Server API KEY

PRIMARY_IP     = "IP"
PRIMARY_PORT   = "PORT"#Port Authoritative Server non API

# Recursor on PRIMARY
PRIMARY_RECURSOR_ZONES_FILE = "/etc/pdns-recursor/recursor.d/local-zones.yml"
# Recursor on SECONDARY — update via SSH
SECONDARY_HOST             = "IP" # Host Secondary PowerDNS Authoritative Server
SECONDARY_RECURSOR_ZONES_FILE = "/etc/pdns-recursor/recursor.d/local-zones.yml"
SECONDARY_SSH_USER         = "root" # User Secondary PowerDNS Authoritative Server
# SSH-key for connect Secondary PowerDNS Authoritative Server
SSH_KEY                    = "~/.ssh/id_rsa"

RECURSOR_FORWARD_TO = "127.0.0.1:PORT"

POLL_INTERVAL  = 30  # seconds
STATE_FILE     = "/var/lib/pdns/zone-sync-state.json"

# =============== Main =================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[
        logging.FileHandler("/var/log/pdns-zone-sync.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)


def pdns_get_zones(api_url, api_key):
    r = requests.get(
        f"{api_url}/api/v1/servers/localhost/zones",
        headers={"X-API-Key": api_key},
        timeout=10
    )
    r.raise_for_status()
    return {z["name"].rstrip("."): z for z in r.json()}


def pdns_create_slave_zone(zone_name, master_ip, master_port, api_url, api_key):
    payload = {
        "name": zone_name + ".",
        "kind": "Secondary",
        "masters": [f"{master_ip}:{master_port}"],
        "nameservers": []
    }
    r = requests.post(
        f"{api_url}/api/v1/servers/localhost/zones",
        headers={"X-API-Key": api_key, "Content-Type": "application/json"},
        json=payload,
        timeout=10
    )
    if r.status_code in (409, 422):
        log.warning(f"Zone {zone_name} already exists on secondary, skipping")
        return False
    r.raise_for_status()
    log.info(f"Created slave zone {zone_name} on secondary")
    return True


def pdns_delete_slave_zone(zone_name, api_url, api_key):
    r = requests.delete(
        f"{api_url}/api/v1/servers/localhost/zones/{zone_name}.",
        headers={"X-API-Key": api_key},
        timeout=10
    )
    if r.status_code == 404:
        log.warning(f"Zone {zone_name} not found on secondary, skipping delete")
        return False
    r.raise_for_status()
    log.info(f"Deleted slave zone {zone_name} from secondary")
    return True


def pdns_notify_zone(zone_name):
    result = subprocess.run(
        ["pdns_control", "notify", zone_name + "."],
        capture_output=True, text=True
    )
    log.info(f"NOTIFY {zone_name}: {result.stdout.strip()}")


def build_recursor_yaml(zone_names):
    """Generate local-zones.yml"""
    lines = []
    for zone in sorted(zone_names):
        lines.append(f'  - zone: "{zone}"')
        lines.append(f'    forwarders:')
        lines.append(f'      - "{RECURSOR_FORWARD_TO}"')
    return "\n".join(lines) + "\n"


def update_recursor_local(zone_names):
    """Update recursor on Primary"""
    content = build_recursor_yaml(zone_names)
    with open(PRIMARY_RECURSOR_ZONES_FILE, "w") as f:
        f.write(content)
    log.info(f"Updated local recursor zones file with {len(zone_names)} zones")

    result = subprocess.run(
        ["rec_control", "reload-zones"],
        capture_output=True, text=True
    )
    log.info(f"Local recursor reload: {result.stdout.strip() or result.stderr.strip()}")


def update_recursor_remote(zone_names):
    """update recursor on Secondary via SSH"""
    content = build_recursor_yaml(zone_names)

    # send content from stdin to SSH
    cmd = [
        "ssh",
        "-i", SSH_KEY,
        "-o", "StrictHostKeyChecking=no",
        "-o", "ConnectTimeout=10",
        f"{SECONDARY_SSH_USER}@{SECONDARY_HOST}",
        f"cat > {SECONDARY_RECURSOR_ZONES_FILE} && rec_control reload-zones"
    ]
    result = subprocess.run(
        cmd,
        input=content,
        capture_output=True,
        text=True
    )
    if result.returncode == 0:
        log.info(f"Remote recursor updated: {result.stdout.strip() or 'ok'}")
    else:
        log.error(f"Remote recursor update failed: {result.stderr.strip()}")


def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f:
            return set(json.load(f))
    return set()


def save_state(zones):
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    with open(STATE_FILE, "w") as f:
        json.dump(list(zones), f)


def main():
    log.info("pdns-zone-sync started")
    known_zones = load_state()

    while True:
        try:
            primary_zones = pdns_get_zones(PRIMARY_API, PRIMARY_KEY)
            current_names = set(primary_zones.keys())

            new_zones     = current_names - known_zones
            deleted_zones = known_zones - current_names

            changed = False

            # --- New zones ---
            if new_zones:
                log.info(f"New zones detected: {new_zones}")
                for zone in new_zones:
                    try:
                        created = pdns_create_slave_zone(
                            zone, PRIMARY_IP, PRIMARY_PORT,
                            SECONDARY_API, SECONDARY_KEY
                        )
                        if created:
                            pdns_notify_zone(zone)
                        # Добавляем в known даже если уже был (409)
                        known_zones.add(zone)
                        changed = True
                    except Exception as e:
                        log.error(f"Failed to add zone {zone}: {e}")

            # --- Deleted zones ---
            if deleted_zones:
                log.info(f"Deleted zones detected: {deleted_zones}")
                for zone in deleted_zones:
                    try:
                        pdns_delete_slave_zone(zone, SECONDARY_API, SECONDARY_KEY)
                        known_zones.discard(zone)
                        changed = True
                    except Exception as e:
                        log.error(f"Failed to delete zone {zone}: {e}")

            # --- Update recursor if new chages ---
            if changed:
                update_recursor_local(known_zones)
                update_recursor_remote(known_zones)
                save_state(known_zones)
            else:
                log.debug("No changes detected")

        except Exception as e:
            log.error(f"Sync cycle error: {e}")

        time.sleep(POLL_INTERVAL)


if __name__ == "__main__":
    main()
