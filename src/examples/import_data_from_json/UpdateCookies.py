import json
import urllib.parse
import urllib.request
import sys

QBDVISION_API_KEY = 'YOUR_API_KEY'
BASE_URL = 'https://api.sandbox.qbdvision.com/sandbox/'
PROJECT_ID = 100  # Update this with your project Id that you want to import the data into
UNIT_OPERATION_ID = 506  # Update this with the Unit Operation Id that you want to update

HEADERS = {
  "qbdvision-api-key": QBDVISION_API_KEY,
  "Content-Type": "application/json"
}

with open('Make Cookies.json') as input_file:
  # Read the incoming JSON file
  process = json.load(input_file)
  print(f"Process size: {len(process)}")
  step = process[0]

  unitOperation = {};
  # Load the existing data from QbDVision
  try:
    req = urllib.request.Request(f"{QBDVISION_URL}/editables/UnitOperation/{UNIT_OPERATION_ID}/{PROJECT_ID}", None,
                                 HEADERS, None, False, "GET")
    with urllib.request.urlopen(req) as response:
      responseText = response.read()
      print(f"Received response: {responseText}\n")
      unitOperation = json.loads(responseText)
  except urllib.error.URLError as err:
    print(f"Request failed! Error: {err.reason}")
    errorContent = err.read().decode("utf-8", "ignore");
    print(f"Error Message: {errorContent}")
    sys.exit()

  # Update the Unit Operation
  unitOperation["name"] = step["step_name"] + " 2";  # QbDVision won't allow updates without changes.

  # Send it back to QbDVision to save
  try:
    data = json.dumps(unitOperation).encode("utf-8")
    print(f"Sending data: {data}\n")
    req = urllib.request.Request(QBDVISION_URL + '/editables/UnitOperation/addOrEdit', data, HEADERS, None, False,
                                 "PUT")
    with urllib.request.urlopen(req) as response:
      responseText = response.read()
      print(f"Received response: {responseText}\n")
  except urllib.error.URLError as err:
    print(f"Request failed! Error: {err.reason}")
    errorContent = err.read().decode("utf-8", "ignore");
    print(f"Error Message: {errorContent}")
