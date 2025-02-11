import json
import urllib.parse
import urllib.request

# Update the following variables with your own values
QBDVISION_API_KEY = 'YOUR_API_KEY'
BASE_URL = 'https://api.sandbox.qbdvision.com/sandbox/'
PROJECT_ID = 100
PROCESS_ID = 111

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
    step = process[stepNum]

    # Create the Unit Operation to be saved.
    unitOperation = {
      "name": step["step_name"],
      "ProjectId": PROJECT_ID,  # Update this with your project Id that you want to import the data into
      "ProcessId": PROCESS_ID,  # Update this with your process Id that you want to import the data into
    }

    if (previousUnitId >= 0):
      unitOperation["PreviousUnitId"] = previousUnitId

    # Send it to QbDVision
    try:
      data = json.dumps(unitOperation).encode("utf-8")
      print(f"\nSending data: {data} to " + BASE_URL + '/editables/UnitOperation/addOrEdit')
      req = urllib.request.Request(BASE_URL + '/editables/UnitOperation/addOrEdit', data, HEADERS, None, False,
                                   "PUT")
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
