FTP-CSV-to-JSON Converter

This repository contains a Node.js script that reads CSV files from an FTP server, converts them to JSON, and maps them to a particular JSON structure before deploying them to an e-commerce tool.

Installation
To install this script, clone the repository and run npm install to install the required dependencies.

Configuration
Before running the script, you will need to configure the following parameters in the config.js file:

ftp: An object containing the FTP server credentials (host, username, and password).
inputPath: The path on the FTP server where the CSV files are located.
outputPath: The path on the local machine where the JSON files should be saved.
jsonStructure: An object defining the desired structure of the JSON files.
Usage
To run the script, use the command npm start. The script will connect to the FTP server, download the CSV files, convert them to JSON, map them to the specified structure, and deploy them to the e-commerce tool. The script will log any errors or warnings to the console.

Dependencies
This script relies on the following Node.js packages:
* adm-zip: A package for working with ZIP archives (used to extract files from the FTP server).
* csvtojson: A package for converting CSV files to JSON.
* node-fetch: A package for making HTTP requests (used to deploy the JSON files to the e-commerce tool).
* config: A package for managing configuration files.
* oauth-1.0a: A package for generating OAuth 1.0a signatures (used to authenticate requests to the e-commerce tool API).
* xmlbuilder: A package for building XML documents (used to construct requests to the e-commerce tool API).

Contributing
If you'd like to contribute to this project, feel free to submit a pull request! Please include detailed information about any changes you've made and how to test them.

License
This repository is licensed under the MIT License. See the LICENSE file for more information.
