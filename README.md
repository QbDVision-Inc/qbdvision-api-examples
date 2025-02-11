# QbDVision API Examples

This repository contains examples and sample code demonstrating how to use the
QbDVision REST API. Each example is self-contained and illustrates a specific
API feature or use case.

## Getting Started

To use these examples, you'll need:

- A QbDVision account with your organization (or
  a [sandbox account](https://sandbox.qbdvision.com/))
- An API
  key ([Get one from your API Keys page](https://sandbox.qbdvision.com/users/list.html?showAPIKeys=true))
- The interpreter for the example you're working with (either in Python 3.13+ or
  Node.js 20.18+)

## Examples

Each directory in this repository focuses on a specific aspect of the API:

- [`/batch_record_api_import`](/src/examples/batch_record_api_import) - How to
  import batch records using the API
- [`/coa_api_import`](/src/examples/coa_api_import) - How to import a
  Certificate of Analysis (CoA) documents/records using the API
- [`/import_data_from_json`](/src/examples/import_data_from_json) - Importing
  basic record data from a JSON file
- [`/process_explorer`](/src/examples/process_explorer) - How to use the Process
  Explorer API to access multiple records

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
```

## Documentation

* REST API Reference
  * This differs by environment, since different environments are different releases (with different features).
    * [Sandbox API Reference](https://sandbox.qbdvision.com/restAPI/index.html)
    * [Professional API Reference](https://login.qbdvision.com/restAPI/index.html)
    * [Professional XR API Reference](https://validated.qbdvision.com/restAPI/index.html)
  
* [API Objects Reference](https://cherrycircle.atlassian.net/wiki/spaces/QK/pages/2412478489/QbDVision+API+Objects) - 
This page provides the list of objects and their attributes that can be used with the API.

## Contributing

We welcome contributions! Please email support@qbdvision.com to get in touch.

## License

This repository is licensed under the MIT License - see the [LICENSE](/LICENSE)
file for details.

## Support

If you have any questions or need help, you can:
* [Open an issue](http://support.qbdvision.com/)
* [Contact our support team](mailto:support@qbdvision.com)

## Disclaimer

These examples are for demonstration purposes only. In production, you should
always follow security best practices and error handling guidelines.
