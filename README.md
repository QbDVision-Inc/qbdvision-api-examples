# QbDVision API Examples

This repository contains examples and sample code demonstrating how to use the
QbDVision REST API. Each example is self-contained and illustrates a specific
API feature or use case.

## Getting Started

To use these examples, you'll need:

- A QbDVision account with your organization (or a [sandbox account](https://sandbox.qbdvision.com/))
- An API key ([Get one from your API Keys page](https://sandbox.qbdvision.com/users/list.html?showAPIKeys=true))
- The interpreter for the example you're workign with (either in Python 3.13+ or Node.js 20.18+)

## Examples

Each directory in this repository focuses on a specific aspect of the API:

- [`/batch_record_api_import`](/batch_record_api_import) - How to import batch records using the API
- [`/coa_api_import`](/coa_api_import) - How to import a Certificate of Analysis (CoA) documents/records using the API
- [`/import_data_from_json`](/import_data_from_json) - Importing basic record data from a JSON file
- [`/process_explorer`](/process_explorer) - How to use the Process Explorer API to access multiple records

## Usage

Each example includes:

- Source code with detailed comments
- A README explaining the specific use case
- Requirements and setup instructions
- Expected output

To run an example:

```bash
# Clone this repository
git clone https://github.com/QbDVision-Inc/qbdvision-api-examples

# Navigate to an example directory
cd qbdvision-api-examples/process_explorer 

# Follow the example-specific README
