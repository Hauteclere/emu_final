import requests
import datetime
import json
import re
import hashlib

TIME_INCREMENT = 10
AIR_DEVICE_IDS = ["eui-a81758fffe0634f4"]
OCCUPANCY_DEVICE_IDS = []
THINGSTACK_BEARER_TOKEN = ""
url = "https://control-data.au1.cloud.thethings.industries/api/v3/as/applications/qut/devices"

def output_data(data_to_output):
    """Prints data to the terminal by default. This function can be modified by the user, depending on the intended data output system."""
    print("Gathering data...")
    print("DATA GATHERED: ")
    print(data_to_output)

def format_data(input):
    """Takes data from the ThingStack API returns and formats it into a JSON array."""
    
    text = input.split("\n")
    
    elements = [json.loads(element) for element in text if element]

    formatted_data = []

    for element in elements:
        if "decoded_payload" in element["result"]['uplink_message'].keys():
            formatted_data.append(
                {
                    **{key: element["result"]['uplink_message']["decoded_payload"][key] for key in element["result"]['uplink_message']["decoded_payload"].keys() & {"co2", "humidity", "temperature"}},
                    "measurement_id": int(hashlib.sha1(f'{element["result"]["end_device_ids"]["device_id"]}{element["result"]["uplink_message"]["received_at"]}'.encode("utf-8")).hexdigest(), 16) % (10 ** 16),
                    "time": element["result"]['uplink_message']["received_at"],
                    "device_id": element["result"]["end_device_ids"]["device_id"]
                }
            )

    return formatted_data


headers = {
    'Authorization': f'Bearer {THINGSTACK_BEARER_TOKEN}',
    'Accept': 'text/event-stream',
}

current_time = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(minutes = TIME_INCREMENT)
timestring = str(current_time.isoformat()[:-6])

device_info = {
    "air_quality": {
        "device_ids": AIR_DEVICE_IDS
    },
    "occupancy": {
        "device_ids": OCCUPANCY_DEVICE_IDS
    }
}

for upload, upload_info in device_info.items():
    print(f"getting data from {upload}")

    data = []
    
    for device in upload_info["device_ids"]:
        data.extend(format_data(requests.get(f'{url}/{device}/packages/storage/uplink_message?after={timestring}Z', headers=headers).text))

    output_data(data)  