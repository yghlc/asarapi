#!/usr/bin/env python
# Filename: asar_download.py 
"""
introduction: download ERS 1 & 2, EnviSat data from esar-ds.eo.esa.int

authors: Huang Lingcao
email:huanglingcao@gmail.com
add time: 10 May, 2023
"""

import os,sys
from optparse import OptionParser
from datetime import datetime
from netrc import netrc
import dateutil.parser

deeplabforRS =  os.path.expanduser('~/codes/PycharmProjects/DeeplabforRS')
sys.path.insert(0, deeplabforRS)

from vector_gpd import shapefile_to_ROIs_wkt

from asarapi.catalog import query

def get_user_password_netrc():
    # Set up authentication using .netrc file
    urs = 'esar-ds.eo.esa.int'  # Address to call for authentication
    netrcDir = os.path.expanduser("~/.netrc")
    user = netrc(netrcDir).authenticators(urs)[0]
    passwd = netrc(netrcDir).authenticators(urs)[2]
    return user, passwd

def save_query_results(results, save_path):
    ## Save results to an output log
    # print(datetime.now(),'Saving log results to ', save_path)
    # convert to json format
    # print(results)

    # save to to file
    results.to_json(save_path,orient='records',date_format='iso', indent=2)
    print(datetime.now(), 'Saved query results to ', save_path)


def download_ASAR_from_ESA(idx, aoi_count, save_dir, aoi_wkt,start, stop, user_name, password, platform=None, product='single-look-complex',orbit=None,
                           polarisation=None, contains=False, limit=500):
    print(datetime.now(), 'Searching... ... ...')
    print(datetime.now(), 'Input search parameters:')
    print('roi_wkt:', aoi_wkt)
    print('save_dir, start_date, end_date:', save_dir, start, stop)
    print('platform, product, flightDirection:', platform, product, orbit)

    results = query(aoi_wkt,start, stop,platform=platform,product=product, orbit=None,polarisation=None,
          contains=contains, limit=limit)

    print(datetime.now(), 'Found %s results' % (len(results)))

    print(datetime.now(), 'Downloading... ... ...')

    if aoi_count == 1:
        download_dir = save_dir
    elif aoi_count > 1:
        download_dir = os.path.join(save_dir, 'aoi_%d' % idx)
    else:
        raise ValueError('There is zero AOI')
    if not os.path.isdir(download_dir):
        os.makedirs(download_dir)

    save_query_results(results,os.path.join(download_dir,'download_data.json'))


    pass

def main(options, args):
    extent_shp = args[0]
    assert os.path.isfile(extent_shp)

    save_dir = options.save_dir
    start_date = dateutil.parser.parse(options.start_date)
    end_date = dateutil.parser.parse(options.end_date)
    user_name = options.username
    password = options.password
    platform = options.dataset_platform
    # flightDirection = options.flightdirection.upper()


    if user_name is None or password is None:
        print('Get user name and password from the .netrc file')
        user_name, password = get_user_password_netrc()

    print(datetime.now(), 'download data from ESA, start_date: %s, end_date: %s, user: %s, \nwill save to %s'%(start_date,end_date,user_name,save_dir))



    if extent_shp.endswith('.txt'):
        print(datetime.now(), "the input is a TXT file")
        file_list_txt = extent_shp
        # download_data_from_asf_list(file_list_txt, save_dir, user_name, password)
        raise ValueError('not support yet')
    else:

        # _check_param(platform, ['ERS', 'Envisat'])
        # _check_param(orbit, ['Ascending', 'Descending'])
        # _check_param(polarisation, ['VV', 'VH', 'HV', 'HH'])

        # shapefile to  ROI
        ROIs_wkt = shapefile_to_ROIs_wkt(extent_shp)
        for idx, roi_wkt in enumerate(ROIs_wkt):
            # download data
            download_ASAR_from_ESA(idx, len(ROIs_wkt),save_dir, roi_wkt, start_date, end_date, user_name, password,
                                   platform=platform, product='single-look-complex',
                                   orbit='all',polarisation='all',contains=False,limit=500)



if __name__ == "__main__":

    usage = "usage: %prog [options] extent_shp or file_ids.txt"
    parser = OptionParser(usage=usage, version="1.0 2022-10-31")
    parser.description = 'Introduction: download data from the Alaska Satellite Facility  '

    parser.add_option("-d", "--save_dir",
                      action="store", dest="save_dir",default='esa_data',
                      help="the folder to save downloaded data")

    parser.add_option("-s", "--start_date",default='2018-04-30',
                      action="store", dest="start_date",
                      help="start date for inquiry, with format year-month-day, e.g., 2018-05-23")
    parser.add_option("-e", "--end_date",default='2018-06-30',
                      action="store", dest="end_date",
                      help="the end date for inquiry, with format year-month-day, e.g., 2018-05-23")

    parser.add_option("", "--platform",
                      action="store", dest="dataset_platform", default='ERS',
                      help="The dataset want to download (Satellite)")

    parser.add_option("", "--flightdirection",
                      action="store", dest="flightdirection",       # default='DESCENDING',
                      help="The flight direction of SAR imagery, Ascending or Descending")

    # parser.add_option("", "--filetype",
    #                   action="store", dest="filetype_product", default='GRD_HD',
    #                   help="The data product want to download, such as GRD_HD or SLC")


    parser.add_option("-u", "--username",
                      action="store", dest="username",
                      help="Earth Data account")
    parser.add_option("-p", "--password",
                      action="store", dest="password",
                      help="password for the earth data account")


    (options, args) = parser.parse_args()
    if len(sys.argv) < 2 or len(args) < 1:
        parser.print_help()
        sys.exit(2)

    main(options, args)