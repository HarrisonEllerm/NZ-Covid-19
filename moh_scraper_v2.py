from requests import get
import os
from bs4 import BeautifulSoup
import requests
import pandas as pd
import json
from geopy.geocoders import Nominatim

# Script used to import Ministry of Health Data into Power BI
# Author: Harry Ellerm

# Path where MOH Data is stored
base_folder = os.path.join(os.path.join(os.environ['USERPROFILE']), 'Documents', 'MOH_Data')
# Base data url for download link
base_data_url = 'https://www.health.govt.nz'
# Current cases url where link exists
current_cases_url = 'https://www.health.govt.nz/our-work/diseases-and-conditions/covid-19-novel-coronavirus/covid-19' \
                    '-current-situation/covid-19-current-cases/covid-19-current-cases-details'
# Location data file
loc_file_path = os.path.join(os.path.join(os.environ['USERPROFILE']), 'Documents', 'MOH_Data', 'Geo_Data',
                             'loc_data.json')
# Geo locator object used for finding lat & long co-ordinates
geo_locator = Nominatim(user_agent='moh_scraper', timeout=10)


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


# Returns the geo-location associated with a location
# Parameter: Name of location i.e. "Wellington"
# Returns: A dictionary of geo location data associated with the location
# i.e. {'location': 'Wellington', 'lat': -41.2887953, 'long': 174.7772114}
def get_geo_loc(location_name, nz):
    # Custom mapping rules - due to geolocater not being able to pick up some DHB's and locations
    if location_name == 'Capital and Coast':
        location_name = 'Wellington'
    elif location_name == 'MidCentral':
        location_name = 'Palmerston North'
    elif location_name == 'Polynesia (excludes Hawaii) nfd':
        location_name = 'Polynesia'
    # If undefined just assign New Zealand as geo-location
    elif location_name == 'TBC':
        location_name = 'New Zealand'

    if nz:
        data = geo_locator.geocode(f'{location_name}, NZ')
    else:
        data = geo_locator.geocode(f'{location_name}')

    if data is None:
        print(f'>> Could not find geo location for "{location_name}", add to custom mapping rules')
        print(f'>> exiting...')
        exit(1)

    return {'location': location_name, 'lat': data.latitude, 'long': data.longitude}


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


# Sets up the location fields within the output data frame
# Queries a json file that holds geo-location data for previously
# queried locations in order to reduce load on Nominatim API
def setup_location_fields(df):
    with open(loc_file_path, 'r') as json_file:
        existing_geo_locs = json.load(json_file)
        lat_list = []
        long_list = []
        arrived_from_lat_list = []
        arrived_from_long_list = []

        # Setup DHB location data
        for index, row in df.iterrows():
            # Already seen this DHB before
            if row['DHB'] in existing_geo_locs:
                lat_list.append(existing_geo_locs[row['DHB']]['lat'])
                long_list.append(existing_geo_locs[row['DHB']]['long'])
            else:
                dhb_location_data = get_geo_loc(row['DHB'], nz=True)
                lat_list.append(dhb_location_data.get('lat'))
                long_list.append(dhb_location_data.get('long'))
                existing_geo_locs[row['DHB']] = {
                    'lat': dhb_location_data.get('lat'),
                    'long': dhb_location_data.get('long')
                }

            # Setup previous country travelled to data
            if row['Last country before return'] == '':
                arrived_from_lat_list.append(None)
                arrived_from_long_list.append(None)
            else:
                # Already seen this country before
                if row['Last country before return'] in existing_geo_locs:
                    arrived_from_lat_list.append(existing_geo_locs[row['Last country before return']]['lat'])
                    arrived_from_long_list.append(existing_geo_locs[row['Last country before return']]['long'])
                else:
                    arrived_from_location_data = get_geo_loc(row['Last country before return'], nz=False)
                    arrived_from_lat_list.append(arrived_from_location_data.get('lat'))
                    arrived_from_long_list.append(arrived_from_location_data.get('long'))
                    existing_geo_locs[row['Last country before return']] = {
                        'lat': arrived_from_location_data.get('lat'),
                        'long': arrived_from_location_data.get('long')
                    }

        with open(loc_file_path, mode='w') as out_file:
            json.dump(existing_geo_locs, out_file)

        return df.assign(DHB_Latitude=lat_list, DHB_Longitude=long_list,
                         Arrived_From_Latitude=arrived_from_lat_list,
                         Arrived_from_Longitude=arrived_from_long_list)


if __name__ == '__main__':
    print('>>> Script started')
    data_source_path = setup_data_source_path()
    path_to_downloaded_file = download_file(from_url=data_source_path, to_folder=base_folder)
    pd.options.display.width = 0

    print(">> Building out confirmed cases...")
    # Read in current cases
    current_cases_raw_df = pd.read_excel(path_to_downloaded_file, sheet_name='Confirmed', engine='openpyxl')
    current_cases_df = pd.DataFrame(current_cases_raw_df.values[3:], columns=current_cases_raw_df.iloc[2])
    # Clean up some rubbish columns
    current_cases_df = current_cases_df.dropna(axis=1, how='all')
    # Makes it easier to test if they travelled to a country prior to contracting
    current_cases_df = current_cases_df.fillna('')
    # Setup location data
    current_cases_df = setup_location_fields(current_cases_df)

    print(">> Building out probable cases...")
    # Read in probable cases
    probable_cases_raw_df = pd.read_excel(path_to_downloaded_file, sheet_name='Probable', engine='openpyxl')
    probable_cases_df = pd.DataFrame(probable_cases_raw_df.values[3:], columns=probable_cases_raw_df.iloc[2])
    probable_cases_df = probable_cases_df.dropna(axis=1, how='all')
    probable_cases_df = probable_cases_df.fillna('')
    probable_cases_df = setup_location_fields(probable_cases_df)

    # Get rid of raw data frames to avoid accidental import into Power-BI
    del current_cases_raw_df
    del probable_cases_raw_df

    # Export for COP
    print('>> Exporting for COP')
    # with pd.ExcelWriter(
    #         f'{data_store_path}\\COP_Data\\Covid_19_Data_For_Cop_{time.strftime("%Y%m%d")}.xlsx') as writer:
    #     current_cases_df.to_excel(excel_writer=writer, sheet_name='Confirmed Cases', index=False)
    #     probable_cases_df.to_excel(excel_writer=writer, sheet_name='Probable Cases', index=False)
    print('>>> Ended')


