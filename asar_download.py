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
from urllib.parse import urlparse
import time
from multiprocessing import Process

deeplabforRS =  os.path.expanduser('~/codes/PycharmProjects/DeeplabforRS')
sys.path.insert(0, deeplabforRS)

from vector_gpd import shapefile_to_ROIs_wkt
import basic_src.io_function as io_function
import basic_src.basic as basic

from asarapi.catalog import query


from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
admin_url = 'https://esar-ds.eo.esa.int/oads/access/login'

machine_name = os.uname()[1]
download_tasks = []

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

def download_one_file_ESA(web_driver, url, save_dir):

    tmp = urlparse(url)
    file_name = os.path.basename(tmp.path)
    save_path = os.path.join(save_dir,file_name)
    if os.path.isfile(save_path):
        print('%s exists, skip downloading'%save_path)

    # check free disk space
    free_GB = io_function.get_free_disk_space_GB(save_dir)
    total_wait_time = 0
    while free_GB < 50 and total_wait_time < 60 * 60 * 12:
        basic.outputlogMessage(' The free disk space (%.4f) is less than 50 GB, wait 60 seconds' % free_GB)
        time.sleep(60)
        total_wait_time += 60
        free_GB = io_function.get_free_disk_space_GB(save_dir)

    web_driver.get(url)

    # wait until the file has been downloaded
    total_wait_time = 0
    while os.path.isfile(save_path) is False and total_wait_time < 60 * 60 * 12:
        time.sleep(60)
        total_wait_time += 60
    basic.outputlogMessage('downloaded: %s'%save_path)


def automated_download_ASAR_ESA(web_driver, data_urls,save_dir, max_process_num=8):

    for ii, url in enumerate(data_urls):
        # download in parallel
        basic.check_exitcode_of_process(download_tasks)  # if there is one former job failed, then quit
        while True:
            job_count = basic.alive_process_count(download_tasks)
            if job_count >= max_process_num:
                print(machine_name, datetime.now(),'You are running %d or more tasks in parallel, wait ' % max_process_num)
                time.sleep(60)  #
                continue
            break

        # start the processing
        sub_process = Process(target=download_one_file_ESA, args=(web_driver,url, save_dir))  # start a process, don't wait
        sub_process.start()
        download_tasks.append(sub_process)

        basic.close_remove_completed_process(download_tasks)

    # wait until all task complete
    while True:
        job_count = basic.alive_process_count(download_tasks)
        if job_count > 0:
            print(machine_name, datetime.now(), 'wait until all task are completed, alive task account: %d ' % job_count)
            time.sleep(60)  #
        else:
            break


def download_ASAR_from_ESA(web_driver, extent_shp, save_dir, start, stop, platform=None, product='single-look-complex',orbit=None,
                           polarisation=None, contains=False, limit=500,process_num=8):

    if not os.path.isdir(save_dir):
        os.makedirs(save_dir)
    ext_base_name = io_function.get_name_no_ext(extent_shp)
    # shapefile to  ROI
    ROIs_wkt = shapefile_to_ROIs_wkt(extent_shp)
    if len(ROIs_wkt) < 1:
        raise ValueError('There is zero AOI')

    for idx, aoi_wkt in enumerate(ROIs_wkt):

        print(datetime.now(), 'Searching... ... ...')
        print(datetime.now(), 'Input search parameters:')
        print('roi_wkt:', aoi_wkt)
        print('save_dir, start_date, end_date:', save_dir, start, stop)
        print('platform, product, flightDirection:', platform, product, orbit)

        results = query(aoi_wkt,start, stop,platform=platform,product=product, orbit=orbit,polarisation=polarisation,
              contains=contains, limit=limit)

        print(datetime.now(), 'Found %s results' % (len(results)))
        print(datetime.now(), 'Downloading... ... ...')

        if len(ROIs_wkt) == 1:
            data_meta_path = os.path.join(save_dir,'%s_meta.json'%ext_base_name)
        else:
            data_meta_path = os.path.join(save_dir, '%s_meta_%d.json' % (ext_base_name, idx))
        save_query_results(results,data_meta_path)

        # download data
        data_urls = results['url'].to_list()
        automated_download_ASAR_ESA(web_driver, data_urls, save_dir, max_process_num=process_num)


def ESA_log_in(save_dir,username,password):
    # log in
    # Step 1: Start a new instance of the Chrome browser using Selenium
    # driver = webdriver.Chrome()
    # Set download directory and options
    options = webdriver.ChromeOptions()
    options.add_experimental_option('prefs', {'download.default_directory': save_dir})
    # Start a new instance of the Chrome browser using Selenium
    driver = webdriver.Chrome(options=options)

    # Step 2: Load the login page
    driver.get(admin_url)
    # Step 3: Wait for the page to load (use a timeout value that makes sense for your website)
    driver.implicitly_wait(15)

    # Step 4: Enter the username and password
    username_field = driver.find_element(By.NAME, 'usernameUserInput')
    password_field = driver.find_element(By.NAME, 'password')
    username_field.send_keys(username)
    password_field.send_keys(password)

    # Step 5: Submit the login form
    password_field.send_keys(Keys.RETURN)

    # Step 6: Wait for the page to load after submitting the form (use a timeout value that makes sense for your website)
    driver.implicitly_wait(10)

    if 'Signed in as %s' % username in driver.page_source:
        print('Login successful!')
        return driver
    else:
        raise ValueError('Login failed!')

def ESA_logout(web_driver):
    LOGOUT_URL = 'https://esar-ds.eo.esa.int/oads/Shibboleth.sso/Logout'
    # Navigate to the logout page
    web_driver.get(LOGOUT_URL)
    web_driver.quit()


def main(options, args):
    extent_shp = args[0]
    assert os.path.isfile(extent_shp)

    save_dir = options.save_dir
    start_date = dateutil.parser.parse(options.start_date)
    end_date = dateutil.parser.parse(options.end_date)
    user_name = options.username
    password = options.password
    platform = options.dataset_platform
    process_num = options.process_num
    # flightDirection = options.flightdirection.upper()


    if user_name is None or password is None:
        print('Get user name and password from the .netrc file')
        user_name, password = get_user_password_netrc()

    print(datetime.now(), 'download data from ESA, start_date: %s, end_date: %s, user: %s, \nwill save to %s'%(start_date,end_date,user_name,save_dir))

    web_driver = ESA_log_in(save_dir,user_name,password)

    if extent_shp.endswith('.txt'):
        print(datetime.now(), "the input is a TXT file")
        file_list_txt = extent_shp
        # download_data_from_asf_list(file_list_txt, save_dir, user_name, password)
        print('not support yet')
    else:

        # (platform, ['ERS', 'Envisat'])
        # (orbit, ['Ascending', 'Descending'])      None for all
        # (polarisation, ['VV', 'VH', 'HV', 'HH'])  None for all

        # download data
        download_ASAR_from_ESA(web_driver, extent_shp, save_dir, start_date, end_date,
                                   platform=platform, product='single-look-complex',
                                   orbit=None,polarisation=None,contains=False,limit=500,process_num=process_num)

    ESA_logout(web_driver)


if __name__ == "__main__":

    usage = "usage: %prog [options] extent_shp or file_ids.txt"
    parser = OptionParser(usage=usage, version="1.0 2023-05-10")
    parser.description = 'Introduction: download data from ESA  '

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

    parser.add_option("", "--process_num",
                      action="store", dest="process_num", type=int,default=8,
                      help="the maximum number of processes for downloading in parallel")


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