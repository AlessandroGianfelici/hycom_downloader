# Hycom downloader

This package provides a simple and user-friendly interface to download data from HYCOM's GLBv0.08 expt_53.X database.

## Installation

This library is pip-installable through the following instruction:

```bash
pip install git+https://github.com/AlessandroGianfelici/hycom_downloader.git
```

## Usage

The "download_data" function is intended as the main interface between the user and the library. 
It takes as input the initial and final dates of the desired time range, the geographical coordinates of the requested point and (optionally)
the list of the feature to be downloaded, if you don't need the full dataset. 

Hereafter a sample of use: 

```python
from hycom_downloader import download_data
from datetime import date

# Defining the sample start and end date
from_date = date(1994, 1, 1)
to_date = date(1994, 1, 3)

# Setting the geographical coordinates
lat = 37.33
lon = 24.00

# The data are downloaded as a pandas dataframe object...
data = download_data(from_date, 
                     to_date, 
                     lat, 
                     lon)
                     
# ... that can be easily dumped using the native .csv method.
data.to_csv("result.csv", index=0)

```

## Feedbacks

Any feedback, improvement/enhancement or issue is welcome in the [issue page](https://github.com/AlessandroGianfelici/hycom_downloader/issues) of the repo.

## Contributing

Feel free to fork this repository, modify the code and open any needed pull requests!

## Licence

This repository has a MIT Licence
