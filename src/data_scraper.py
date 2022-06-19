import requests
import datetime
import json
import hashlib

def format_data(input):
    """Takes data from the ThingStack API returns and formats it into a JSON array."""
    
    text = input.split("\n")
    
    elements = [json.loads(element) for element in text if element]

    formatted_data = []

    for element in elements:
        try:
            if "decoded_payload" in element["result"]['uplink_message'].keys():
                formatted_data.append(
                    {
                        **{key: element["result"]['uplink_message']["decoded_payload"][key] for key in element["result"]['uplink_message']["decoded_payload"].keys() & {"co2", "humidity", "temperature"}},
                        "measurement_id": int(hashlib.sha1(f'{element["result"]["end_device_ids"]["device_id"]}{element["result"]["uplink_message"]["received_at"]}'.encode("utf-8")).hexdigest(), 16) % (10 ** 16),
                        "time": element["result"]['uplink_message']["received_at"],
                        "device_id": element["result"]["end_device_ids"]["device_id"]
                    }
                )
        except:
            pass

    return formatted_data


headers = {
    'Authorization': 'Bearer NNSXS.6NP6PQAGTHKHTI52ABIKKTGQSYPMBRLE5FUA53A.SABIGFAW5DS3HOXNQHHIB3W6HSVC6QFXOMHKKFFEHFGM7SX2VQ5Q',
    'Accept': 'text/event-stream',
}

current_time = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(minutes = 20)
timestring = str(current_time.isoformat()[:-6])

url = "https://control-data.au1.cloud.thethings.industries/api/v3/as/applications/qut/devices"

device_info = {
    "air_quality": {
        "device_ids": ["eui-a81758fffe0634f4"]
    },
}

for upload, upload_info in device_info.items():
    print(f"getting data from {upload}")

    data = []
    
    for device in upload_info["device_ids"]:
        data.extend(format_data(requests.get(f'{url}/{device}/packages/storage/uplink_message?after={timestring}Z', headers=headers).text))
    print("Gathering data...")
    print("DATA GATHERED: ")
    print(data)  