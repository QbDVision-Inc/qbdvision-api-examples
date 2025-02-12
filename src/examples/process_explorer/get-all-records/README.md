# Get all Process Explorer Records Example (Node.js)

This example demonstrates how to load all of the records in a process using the QbDVision REST API with Node.js.

## Prerequisites

- Node.js 20.x or higher
- A QbDVision API key

## Getting Started

1. Install the dependencies
```bash
npm install
```
2. Copy `.env.example` to `.env` and fill in:
    1. Modify `API_Key` to be your QbDVision API key.
    2. Modify `BASE_URL` to point to your QbDVision environment if you're not using Sandbox.
    3. Modify `PROJECT_ID` and `PROCESS_ID` to be the project and process ID that you want to see.
3. Run the example:
```bash
npm run start
````

## Output
The output will look something like this, with the full results written to a file named `all-records.json`:

```
Ryans-MacBook-Pro:get-all-records ryanshillington$ npm run start

> get-all-records@0.1.0 start
> node src/index.js

Sending request to get process explorer data...
Found the following keys:  [
  'UO-1646',  'UO-1647',  'UO-1648',
  'UO-1649',  'UO-1650',  'UO-1651',
  'UO-1652',  'UO-1653',  'STP-1256',
  'STP-1257', 'STP-1258', 'STP-1259',
  'STP-1281', 'MT-1999',  'MT-2000',
  'MT-2002',  'MT-2024',  'MT-2026',
  'PRC-2111', 'PRC-2112', 'PRC-2113',
  'PP-7172',  'PP-7173',  'MA-4434',
  'MA-4435',  'MA-4436',  'MA-4437',
  'MA-4438',  'MA-4439',  'MA-4440',
  'MA-4441',  'MA-4442',  'MA-4443',
  'IQA-3301', 'IQA-3302', 'IQA-3303'
]
Sending request to get record data...
Received 36 records:
  UO-1651: Mix the ingredients
  UO-1648: Equipment Check
  UO-1647: Packaging
  UO-1646: Set everything up
  UO-1653: Top off with water
  UO-1652: Pour into a glass with Ice cubes
  UO-1650: Mix Ingredients
  UO-1649: Add Coffee, Water and Sugar
  STP-1281: Add Milk to the mixture
  STP-1259: Check Equipment
  STP-1258: Test Ingredients
  STP-1257: Gather Materials
  STP-1256: Stir it all
  MT-2026: Ice
  MT-2024: Sugar
  MT-2002: Milk
  MT-2000: Water
  MT-1999: Coffee beans
  PRC-2113: Glass
  PRC-2112: Stirrer
  PRC-2111: Scale
  PP-7172: Stirring time
  PP-7173: Quantity
  MA-4443: Oxygen content
  MA-4441: Temperature
  MA-4442: Purity
  MA-4440: pH Level
  MA-4434: Type of milk
  MA-4436: Temperature
  MA-4435: Fat content
  MA-4437: Type
  MA-4439: Moisture
  MA-4438: Density
  IQA-3303: Water to Mix Ratio
  IQA-3302: Mixture Homogeneity
  IQA-3301: Sugar to Coffee Ratio
Writing the data to all-records.json...
Done!
```

# Best Practices for Production
- Use a key store for API keys
- Implement proper error handling and retries
- Add logging/reporting according to your company's standards
- Use the latest versions of all libraries
