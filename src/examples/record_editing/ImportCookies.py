import json
import urllib.parse
import urllib.request

QBDVISION_API_KEY = 'xxxYourAPIKeyGoesHerexxx'
QBDVISION_URL = 'https://api.dev-staging-circleci.qbdvision.com/dev-staging-circleci'
HEADERS = {
    "qbdvision-api-key": QBDVISION_API_KEY, 
    "Content-Type": "application/json",
}

with open('Make Cookies.json') as input_file:

    # Read the incoming JSON file
    process = json.load(input_file)
    print(f"Process size: {len(process)}")
    previousUnitId = -1;
    for stepNum in range(len(process)):
        riffynStep = process[stepNum]

        # Create the Unit Operation to be saved.
        unitOperation = {
            "name": riffynStep["step_name"],
            "ProjectId": 100,  # Update this with your project Id that you want to import the data into
            "ProcessId": 111,  # Update this with your process Id that you want to import the data into
        }

        if (previousUnitId >= 0):
            unitOperation["PreviousUnitId"] = previousUnitId

        # Send it to QbDVision
        try:
            data = json.dumps(unitOperation).encode("utf-8")
            print(f"\nSending data: {data} to " + QBDVISION_URL + '/editables/UnitOperation/addOrEdit')
            req = urllib.request.Request(QBDVISION_URL + '/editables/UnitOperation/addOrEdit', data, HEADERS, None, False, "PUT")
            with urllib.request.urlopen(req) as response:
                responseText = response.read()
                print(f"Received response for importing {unitOperation['name']}: {responseText}\n")
                responseUO = json.loads(responseText);
                previousUnitId = responseUO["id"];
        except urllib.error.URLError as err:
            print(f"Request failed! Error: {err}")
            errorContent = err.read().decode("utf-8", "ignore");
            print(f"Error Message: {errorContent}")
            break;



