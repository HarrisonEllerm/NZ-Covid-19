from requests import get
import os
from bs4 import BeautifulSoup
import platform
import requests
import pandas as pd
from geopy.geocoders import Nominatim

# Script used to import Ministry of Health Data into Power BI
# Author: Harry Ellerm

# Path where MOH Data is stored if script is running on a Windows system
windows_data_loc = os.path.join(os.path.join(os.environ['USERPROFILE']), 'Documents', 'MOH_Data')
# Path where MOH Data is stored if script is running on a Mac or Linux System
unix_data_loc = os.path.join(os.path.join(os.path.expanduser('~')), 'Documents', 'MOH_Data')
# Base data url for download link
base_data_url = 'https://www.health.govt.nz'
# Current cases url where link exists
current_cases_url = 'https://www.health.govt.nz/our-work/diseases-and-conditions/covid-19-novel-coronavirus/covid-19' \
                    '-current-situation/covid-19-current-cases/covid-19-current-cases-details'
# List of geo data for locations
geo_dict_list = []
# Geo locator object used for finding lat & long co-ordinates
geo_locator = Nominatim(user_agent='mohscraper', timeout=5)


# Sets up folder to store data within. If it doesn't already exist
# a folder is created to store the data. Returns associated folder path.
def setup_environ():
    if platform.system() == 'Darwin' or platform.system() == 'Linux':
        folder_path = unix_data_loc
    else:
        folder_path = windows_data_loc
    try:
        if os.path.exists(folder_path):
            print(f'>> Folder already exists: {folder_path}')
        else:
            os.makedirs(folder_path)
            print(f'>> Creating: {folder_path} to store data')
        return folder_path
    except OSError:
        print(f'>> Error creating folder to storing MOH Data... exiting')
        exit(1)


def setup_data_source_path():
    end_url = ''
    # Used to escape the leading 0 of a month
    # i.e. to get 1 instead of 01 for the first day of the month
    # Solution varies between platforms
    # See: https://stackoverflow.com/questions/904928/python-strftime-date-without-leading-0
    test_request = requests.get(current_cases_url)
    # If web page is live search for link to download data
    if test_request.status_code == 200:
        response = requests.get(current_cases_url)
        soup = BeautifulSoup(response.text, 'html.parser')
        for link in soup.findAll('a', href=True):
            if 'Download confirmed and probable case data' in link.text:
                end_url = link['href']
        # This may be 0 if the above string could not be found in an anchors text
        if len(end_url) > 0:
            print(f'>> Found data source at {base_data_url + end_url}')
            return base_data_url + end_url
        else:
            print(f'>> Web page was live at {current_cases_url} but could not find download link... exiting')
            exit(1)

    else:
        print(f'>> Could not find current cases web-page, check that the link {current_cases_url} is still valid... '
              f'exiting')
        exit(1)


# Returns the geo-location associated with a DHB
# If it is already within the list of DHBs it pulls it from the list
# Otherwise it uses gepoy to find it, and updates the list
# Parameter: Name of location i.e. "Wellington"
# Returns: A dictionary of geo location data associated with the location
# i.e. {'location': 'Wellington', 'lat': -41.2887953, 'long': 174.7772114}
def get_geo_loc(location_name, nz):
    # Custom mapping rules - due to geolocater noo being able to pick up some DHBs
    if location_name == 'Capital and Coast':
        location_name = 'Wellington'
    elif location_name == 'MidCentral':
        location_name = 'Palmerston North'
    # If undefined just assign New Zealand as geo-location
    elif location_name == 'TBC':
        location_name = 'New Zealand'

    # If the list has items and data for the location we are looking for already exists, return it
    if len(geo_dict_list) > 0 and len([loc for loc in geo_dict_list if loc['location'] == location_name]) > 0:
        return [loc for loc in geo_dict_list if loc['location'] == location_name][0]
    # Otherwise query for it, store it in the dictionary and then return data
    else:
        if nz:
            data = geo_locator.geocode(f'{location_name}, NZ')
        else:
            data = geo_locator.geocode(f'{location_name}')

        geo_dict = {'location': location_name,
                    'lat': data.latitude,
                    'long': data.longitude}
        geo_dict_list.append(geo_dict)
        return geo_dict


# Downloads a file using requests and writes it
# to an output file within the data store folder
# Returns full path to the downloaded file
def download_file(from_url, to_folder):
    file_name_from_url = os.path.basename(from_url)
    file_loc = to_folder + '\\' + file_name_from_url
    new_file = open(file_loc, 'wb')
    response = get(from_url)
    new_file.write(response.content)
    return file_loc


def setup_location_fields(df):
    lat_list = []
    long_list = []
    arrived_from_lat_list = []
    arrived_from_long_list = []
    # Setup DHB location data
    for index, row in df.iterrows():
        dhb_location_data = get_geo_loc(row['DHB'], nz=True)
        lat_list.append(dhb_location_data.get('lat'))
        long_list.append(dhb_location_data.get('long'))

        if row['Last country before return'] is None:
            arrived_from_lat_list.append(None)
            arrived_from_long_list.append(None)
        else:
            arrived_from_location_data = get_geo_loc(row['Last country before return'], nz=False)
            arrived_from_lat_list.append(arrived_from_location_data.get('lat'))
            arrived_from_long_list.append(arrived_from_location_data.get('long'))

    df = df.assign(DHB_Latitude=lat_list, DHB_Longitude=long_list,
                   Arrived_From_Latitude=arrived_from_lat_list,
                   Arrived_from_Longitude=arrived_from_long_list)
    return df


if __name__ == '__main__':
    print('>>> Script started')
    data_store_path = setup_environ()
    data_source_path = setup_data_source_path()
    path_to_downloaded_file = download_file(from_url=data_source_path, to_folder=data_store_path)

    pd.options.display.width = 0

    # Read in current cases
    current_cases_raw_df = pd.read_excel(path_to_downloaded_file, sheet_name='Confirmed', engine='openpyxl')
    current_cases_df = pd.DataFrame(current_cases_raw_df.values[3:], columns=current_cases_raw_df.iloc[2])
    # Setup location data
    current_cases_df = setup_location_fields(current_cases_df)
    # Clean up some rubbish columns
    current_cases_df = current_cases_df.dropna(axis=1, how='all')

    # Read in probable cases
    probable_cases_raw_df = pd.read_excel(path_to_downloaded_file, sheet_name='Probable', engine='openpyxl')
    probable_cases_df = pd.DataFrame(probable_cases_raw_df.values[3:], columns=probable_cases_raw_df.iloc[2])
    # Setup location data
    probable_cases_df = setup_location_fields(probable_cases_df)
    # Clean up
    probable_cases_df = probable_cases_df.dropna(axis=1, how='all')
