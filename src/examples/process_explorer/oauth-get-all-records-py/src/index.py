from utils.open_api_proxy import OpenAPIProxy
import json
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

BASE_URL = os.getenv('BASE_URL')
PROJECT_ID = os.getenv('PROJECT_ID')
PROCESS_ID = os.getenv('PROCESS_ID')

def convert_map_to_array_of_keys(map_data):
    """Convert a map to an array of keys."""
    if not map_data:
        return []
    
    objects = [obj for obj in map_data.values() if not obj.get('deletedAt')]
    return [f"{obj['typeCode']}-{obj['id']}" for obj in objects]

def main():
    """Main function to run the program."""
    open_api_proxy = OpenAPIProxy(BASE_URL)

    print("Logging in...")
    open_api_proxy.login()

    print("Sending request to get process explorer data...")
    # In Python, we'll use query parameters as a dictionary
    result = open_api_proxy.get(f"/processExplorer/{PROJECT_ID}", {"processId": PROCESS_ID})

    all_record_keys = []

    # Get data from various maps
    data = result.get("data", {})
    all_record_keys.extend(convert_map_to_array_of_keys(data.get("uoMap", {})))
    all_record_keys.extend(convert_map_to_array_of_keys(data.get("stpMap", {})))
    all_record_keys.extend(convert_map_to_array_of_keys(data.get("mtMap", {})))
    all_record_keys.extend(convert_map_to_array_of_keys(data.get("prcMap", {})))
    all_record_keys.extend(convert_map_to_array_of_keys(data.get("ppMap", {})))
    all_record_keys.extend(convert_map_to_array_of_keys(data.get("maMap", {})))
    all_record_keys.extend(convert_map_to_array_of_keys(data.get("iqaMap", {})))
    all_record_keys.extend(convert_map_to_array_of_keys(data.get("ipaMap", {})))

    print(f"Found the following keys: {all_record_keys}")

    print("Sending request to get record data...")
    params = {
        "onlyLoadDetails": "true",
        "includeHistory": "true",
        "includeClonedFrom": "true",
        "shouldCompress": "true",
        "includeFromLibrary": "true",
        "includeRecordOrder": "true",
        "returnLatestApprovedOrDraftVersionsOnly": "true"
    }

    data_result = open_api_proxy.put(
        f"/editables/multiple/list/{PROJECT_ID}",
        {"requirementIds": all_record_keys},
        params=params
    )

    instances = data_result.get("data", {}).get("instances", [])
    print(f"Received {len(instances)} records:")

    for instance in instances:
        print(f"  {instance.get('typeCode')}-{instance.get('id')}: {instance.get('name')}")

    print("Writing the data to all-records.json...")
    with open("all-records.json", "w") as f:
        json.dump(instances, f, indent=2)
    
    print("Done!")

main()
