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
# Location data file
loc_file_path = os.path.join(os.path.join(os.environ['USERPROFILE']), 'Documents', 'MOH_Data', 'Geo_Data',
                             'loc_data.json')

# Base data url for download link
base_data_url = 'https://www.health.govt.nz'
# URL used to grab key summary stats from summary table
summary_stats_url = 'https://www.health.govt.nz/our-work/diseases-and-conditions/covid-19-novel-coronavirus/' \
                    'covid-19-current-situation/covid-19-current-cases'
# Current cases url where download link exists for data-set
current_cases_url = 'https://www.health.govt.nz/our-work/diseases-and-conditions/covid-19-novel-coronavirus/covid-19' \
                    '-current-situation/covid-19-current-cases/covid-19-current-cases-details'

# Geo locator object used for finding lat & long co-ordinates
geo_locator = Nominatim(user_agent='moh_scraper', timeout=10)


def setup_data_source_path():
    """
    Sets up the data source path (path to download link).

    Scrapes web page to find all links then looks for text  matching link description
    'Download confirmed and probable case data' on the MOH web page.

    If the web page isn't live (or the link has changed) an HTTP status code will be returned
    other than 200 'OK'. In this instance the script will exit (by design) as the link will
    need to be updated.

    :return: download URL for MOH COVID-19 data file
    """
    end_url = ''
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
        print(f'>> Could not find web-page, check that the link {current_cases_url} is still valid... '
              f'exiting')
        exit(1)


def get_key_stats():
    """
    Pulls key stats from the MOH web-page (Summary Table)

    If the web page isn't live (or the link has changed) an HTTP status code will be returned
    other than 200 'OK'. In this instance the script will exit (by design) as the link will
    need to be updated.

    :return: a data-frame containing stats for:
       - Number of cases currently in hospital
       - Number of recovered cases
       - Number of deaths
       - Increase in confirmed cases within 24 hours
       - Increase in probable cases within 24 hours
    """
    test_request = requests.get(summary_stats_url)
    # If web page is live search for link to download data
    if test_request.status_code == 200:
        response = requests.get(summary_stats_url)
        soup = BeautifulSoup(response.text, 'html.parser')
        tables = soup.findAll('table')
        # Build out summary stats table - the first table in this web page
        raw_summary_stats_df = pd.read_html(str(tables[0]))[0]
        raw_summary_stats_df.columns = ['Statistic', 'Total to date', 'New in last 24 hours']
        # Transpose
        raw_summary_stats_df = raw_summary_stats_df.set_index('Statistic').T
        # Perform a few transformations to get data into format I want it in
        summary_stats_df = raw_summary_stats_df.loc[['Total to date'], ['Number of cases currently in hospital',
                                                                        'Number of recovered cases',
                                                                        'Number of deaths']]
        new_in_last_24_hours = raw_summary_stats_df.loc[
            ['New in last 24 hours'], ['Number of confirmed cases in New Zealand',
                                       'Number of probable cases']]
        summary_stats_df['Increase in confirmed cases within 24 hours'] = new_in_last_24_hours.at[
            'New in last 24 hours',
            'Number of confirmed '
            'cases in New Zealand']
        summary_stats_df['Increase in probable cases within 24 hours'] = new_in_last_24_hours.at[
            'New in last 24 hours',
            'Number of probable cases']
        return summary_stats_df

    else:
        print(f'>> Could not find web-page, check that the link {summary_stats_url} is still valid... ')
        print(f'>> exiting...')
        exit(1)


def get_geo_loc(location_name, nz=False):
    """
    Returns the geo-location data (lat, long) associated with a location
    i.e. {'location': 'Wellington', 'lat': -41.2887953, 'long': 174.7772114}

    If a geo-location data is unable to be found for a location the script exits. This is
    by design, the name of the location will have to be added to the custom mapping rules
    below (see script output to find the location name).

    :param location_name: the name of the location i.e. 'Auckland'
    :param nz: True if the location is in NZ or False if not
    :return: a dictionary containing the location name, latitude & longitude
    """
    # Custom mapping rules - due to geo-locator not being able to pick up some DHB's and locations
    if location_name == 'Capital and Coast':
        location_name = 'Wellington'
    elif location_name == 'MidCentral':
        location_name = 'Palmerston North'
    elif location_name == 'Polynesia (excludes Hawaii)':
        location_name = 'Polynesia'
    elif location_name == 'Polynesia (excludes Hawaii) ':
        location_name = 'Polynesia'
    # As South Canterbury is an informal name given to an area within
    # Canterbury - set it to a town in the middle the informal area
    elif location_name == 'South Canterbury':
        location_name = 'Fairlie'
    # If undefined just assign New Zealand as geo-location
    elif location_name == 'TBC':
        location_name = 'New Zealand'

    if nz:
        # Have found that appending 'NZ' to the location name improves location data accuracy
        # This problem doesn't persist for international locations as they are recorded as just a
        # country name rather than a region within NZ
        data = geo_locator.geocode(f'{location_name}, NZ')
    else:
        data = geo_locator.geocode(f'{location_name}')

    if data is None:
        print(f'>> Could not find geo location for "{location_name}", add to custom mapping rules')
        print(f'>> exiting...')
        exit(1)

    print(f'>> Found new location {location_name}...')

    return {'location': location_name, 'lat': data.latitude, 'long': data.longitude}


def download_file(from_url, to_folder):
    """
    Downloads a file and writes it to an output file within the base folder.

    :param from_url: the url to download the file from
    :param to_folder: the folder to write the output file to
    :return: the path to the file created
    """
    file_name_from_url = os.path.basename(from_url)
    file_loc = os.path.join(to_folder, file_name_from_url)
    new_file = open(file_loc, 'wb')
    response = get(from_url)
    new_file.write(response.content)
    return file_loc


def setup_locs(df):
    """
    Sets up the location fields within the output DataFrames.

    Queries a persistent JSON file that holds geo-location data for
    previously queried locations. This helps reduce the number of queries
    to the Nominatim API and improves the speed of the script.

    Four location fields are setup:

    DHB_Latitude: The latitude of the DHB where this case was reported
    DHB_Longitude: The longitude of the DHB where this case was reported
    Arrived_From_Latitude: The latitude of the country where this case arrived from, if any
    Arrived_From_Longitude: The longitude of the country where this case arrived from, if any

    :param df: the data-frame to which the four location fields above are being added
    :return: the modified data-frame

    TODO: This looping isn't great, maybe refactor (vectorise)? Although relatively small number of rows...
    """

    with open(loc_file_path, 'r+') as json_file:
        existing_geo_locs = json.load(json_file)
        orig_num_existing_locs = len(existing_geo_locs)
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
            # Doesn't make sense to have international travel as 'yes' if last country travelled to is recorded
            # as New Zealand
            if pd.isna(row['Last country before return']) or row['Last country before return'] == 'New Zealand':
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

        # Overwrite json file with new content if there is any
        if len(existing_geo_locs) > orig_num_existing_locs:
            json_file.seek(0)
            json_file.truncate()
            json.dump(existing_geo_locs, json_file)

        return df.assign(DHB_Latitude=lat_list, DHB_Longitude=long_list,
                         Arrived_From_Latitude=arrived_from_lat_list,
                         Arrived_From_Longitude=arrived_from_long_list)


def setup_case_data(path_to_wb, sheet_name, confirmed_or_probable_identifier):
    """
    Reads in data from the downloaded workbook and cleans it up

    :param path_to_wb: Path to the excel workbook being read in
    :param sheet_name: Name of the sheet being read in from the workbook
    :param confirmed_or_probable_identifier: 1 if the case is confirmed, 0 if the case is probable
    :return: the case data data-frame
    """
    # Read in case data
    df = pd.read_excel(path_to_wb, sheet_name=sheet_name, engine='openpyxl', skiprows=3)
    # Setup location data
    df = setup_locs(df)
    # Setup confirmed/probable identifier
    df['Confirmed_or_Probable'] = confirmed_or_probable_identifier
    return df


# Note to self - do not include this in external main() as Power-BI cannot run it unless it is purely a script
# Power-BI will detect the data-frames created by this script (cases_df & summary_df) and provide the option to
# import them after the script has finished running.
if __name__ == '__main__':
    print('>>> Script started')
    # Get link to COVID-19 data file and download it if it exists
    data_source_path = setup_data_source_path()
    path_to_downloaded_file = download_file(from_url=data_source_path, to_folder=base_folder)

    print(">> Building out case data...")
    current_cases_df = setup_case_data(path_to_downloaded_file, 'Confirmed', 1)
    probable_cases_df = setup_case_data(path_to_downloaded_file, 'Probable', 0)

    # --------------------------------------- Merge into all cases --------------------------------------- #
    # It is easier to merge the two sources (current_cases and probable_cases) together and setup an
    # identifier for the case being probable or confirmed because of the way Power-BI works when linking tables
    # together via relationships. If, for example, you wanted to plot confirmed and probable cases over time (as I do)
    # you would need to link these tables together using a date table (as there isn't always a confirmed/probable
    # case that occurred on the same day. This is fine, but then when you also want to link the DHB fields together
    # by setting up a relationship between the current cases table and probable cases table Power-BI wont let you
    # as there is already a link between the tables (via the date fields linking to the date table).
    # There is ways you can 'activate' certain relationships but this solution is generally easier here IMO.
    # ---------------------------------------------------------------------------------------------------- #
    cases_df = current_cases_df.append(probable_cases_df, ignore_index=True)

    # Grab summary stats
    print(">> Building out summary stats...")
    summary_df = get_key_stats()

    # Get rid original data-frames (as we merged them into one above) to avoid accidental import into Power-BI
    del current_cases_df
    del probable_cases_df

    # Export for COP
    print('>> Exporting for COP')
    with pd.ExcelWriter(f'{base_folder}\\COP_Export\\Covid_19_Data_For_Cop.xlsx') as writer:
        cases_df.to_excel(excel_writer=writer, sheet_name='Confirmed and Probable Cases', index=False)
        summary_df.to_excel(excel_writer=writer, sheet_name='Summary Stats', index=False)
    print('>>> Ended')
