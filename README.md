# Spark task - EPAM Data Engineering
By Zhumakhan Arman

- The task code is in `main.py`
- Util functions are in `geo_utils.py`

## How to run the code:

1. Create a virtual environment

`python -m venv venv`

2. Install necessary packages

`pip install -r requirements.txt`

3. Download the weather data in the project folder and unzip.
- If it's already downloaded, you can set its path in the `config.py` file
- If zip files are not combined, run the following code and unzip the output file.

`python combine_zip_files.py`

4. Download the restaurant data and unzip.
- If it's already downloaded, you can set its path in the `config.py` file

5. Run the code

`python main.py`