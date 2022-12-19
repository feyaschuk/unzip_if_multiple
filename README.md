﻿# unzip_if_multiple.py

### Description:

The script decompresses a file that is received in a "raw" folder in Azure Blob Storage and has a specific prefix.
Extracts the csv file and renames it, adding 2 digits to the year if needed.
A Zip file in a "raw" folder can be filtered by date.

### How to use: 
* Clone the repository and go to it on the command line:

```bash
git clone https://github.com/feyaschuk/unzip_if_multiple.py.git
```
```bash
cd unzip_if_multiple.py
```

* Create and activate virtual environment:
```bash
python3 -m venv env
```
```bash
source env/bin/activate (MAC OC, Linux) // source venv/Scripts/activate (Windows)
```
```bash
python3 -m pip install --upgrade pip
```

* Install dependencies from requirements.txt file:
```bash
pip install -r requirements.txt
```

* Run the program:
```bash
python unzip_if_multiple.py filter_date prefix container_name
```

#### Example of usage:
```bash
python unzip_if_multiple.py '20221129' 'raw/PL_SIC_' 'da1-p23-r-eagle-dropdir'
```
