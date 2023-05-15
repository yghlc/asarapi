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
from datetime import timedelta
from netrc import netrc
import dateutil.parser
from urllib.parse import urlparse
import time
import re
from multiprocessing import Process

deeplabforRS =  os.path.expanduser('~/codes/PycharmProjects/DeeplabforRS')
sys.path.insert(0, deeplabforRS)

import vector_gpd
from vector_gpd import shapefile_to_ROIs_wkt
from vector_gpd import wkt_string_to_polygons
import basic_src.io_function as io_function
import basic_src.basic as basic

from asarapi.catalog import query
import pandas as pd

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
admin_url = 'https://esar-ds.eo.esa.int/oads/access/login'

machine_name = os.uname()[1]
download_tasks = []

b_rm_small_overlap = True

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
    out_res = results.reset_index()
    out_res.to_json(save_path,orient='records',date_format='iso', indent=2)
    print(datetime.now(), 'Saved query results to ', save_path)

def create_soft_link(dir,old_filename, new_filename):
    current_dir = os.getcwd()
    os.chdir(dir)
    cmd_str = 'ln -s %s %s '%(new_filename, old_filename)
    basic.os_system_exit_code(cmd_str)
    os.chdir(current_dir)

def does_ERS_file_exist(file_name, dir_name):
    # it's strange that download file name of ERS imagery is different the filename in the URL and ID.
    # for example,
    # in the url, the file name is: SAR_IMS_1PNESA20041031_203036_00000015A099_00386_49839_0000.E2
    # but after downloaded, the filename is: SAR_IMS_1PNESA20041031_203035_00000018A099_00386_49839_0000.E2
    # difference: 203036 changed to 203035,  00000015A099 changed to 00000018A099,
    # 203036 means the acquisition time is 20:30:36
    # 00000015A099 is Unique identifier for the specific image acquisition

    save_path = os.path.join(dir_name,file_name)
    if os.path.isfile(save_path):
        return True

    datetime_format = '%Y%m%d_%H%M%S'

    # try to search similar file names
    diff = [i for i in range(-5,6)]
    # diff.remove(0)
    strs = file_name.split('_')
    # change_term = int(strs[3]) # 203036
    change_term_str = strs[2][6:]+'_'+strs[3]       # 20041031_203035
    change_term = datetime.strptime(change_term_str, datetime_format)
    strs[4] = '*'       # 00000015A099 change to something, not sure what it look like.
    for ii in diff:
        # add or substract a few second
        # tmp = str(change_term + ii)
        tmp = change_term + timedelta(seconds=ii)
        tmp_str = tmp.strftime(datetime_format)
        tmp_str_list = tmp_str.split('_')
        strs[2] = strs[2].replace(strs[2][6:], tmp_str_list[0]) # replace 20041031 to a new date if happen
        strs[3] = tmp_str_list[1]                               # change 203035 to a new time after adding a new seconds

        new_name = '_'.join(strs)
        file_list = io_function.get_file_list_by_pattern(dir_name,new_name)

        # remove files are links or has created links
        if len(file_list) > 1:
            f_name_list = [ os.path.basename(item) for item in file_list]
            rm_idxs = []
            for idx, (f_name, f_path) in enumerate(zip(f_name_list,file_list)):
                if os.path.islink(f_path):
                    target = os.readlink(f_path)
                    rm_idxs.append(idx)
                    if target in f_name_list:
                        rm_idxs.append(f_name_list.index(target))
            file_list = [ file_list[idx] for idx in range(len(file_list)) if idx not in rm_idxs]

        # print(new_name)
        # if os.path.isfile(new_path):
        #     basic.outputlogMessage('Warning, %s does not exists, but a file with similar name exists: %s'%(file_name,new_name))
        #     return True
        if len(file_list) == 1:
            basic.outputlogMessage('Warning, %s does not exist, but a file with similar name exists: %s' % (file_name, file_list[0]))
            create_soft_link(dir_name,file_name, os.path.basename(file_list[0]))
            return True

    return False


def test_does_ERS_file_exist():
    # file_name = 'SAR_IMS_1PNESA20041031_203036_00000015A099_00386_49839_0000.E2'
    # dir_name = os.path.expanduser('~/Data/asar_ERS_Envisat')
    file_name = 'ASA_IMS_1PNESA20050405_181353_000000152036_00113_16199_0000.N1'
    dir_name = os.path.expanduser('~/Data/asar_ERS_Envisat/Envisat')
    print(does_ERS_file_exist(file_name, dir_name))

def remove_record_only_cover_parts(query_results, aoi_wkt):
    aoi_poly = wkt_string_to_polygons(aoi_wkt)
    footprints = query_results['footprint'].to_list()
    if len(footprints) < 1 or b_rm_small_overlap is False:
        return query_results
    footprint_polys = [wkt_string_to_polygons(item) for item in footprints]

    sel_idx = []
    for idx, fp in enumerate(footprint_polys):
        intersection = aoi_poly.intersection(fp)
        if intersection.area < fp.area/5 and intersection.area < aoi_poly.area/2:
            continue
        else:
            sel_idx.append(idx)
        # if intersection.is_empty is True:
        #     return 0.0

    # # test, saving to shapefile
    # save_shp_path = 'query_results.gpkg'
    # save_q_results = query_results.copy()
    # save_q_results['outline'] = footprint_polys
    # # 'ESRI Shapefile' dont support datetime format, but GPKG do
    # vector_gpd.save_polygons_to_files(save_q_results,'outline','EPSG:4326',save_shp_path,format='GPKG')
    # print('saved %s'%save_shp_path)

    basic.outputlogMessage('Originally found %d SAR images, removed %d records that only cover a small portions of the study area'
                           %(len(footprint_polys), (len(footprint_polys) - len(sel_idx))))

    return query_results.iloc[sel_idx]


def download_one_file_ESA(web_driver, url, save_dir):

    tmp = urlparse(url)
    file_name = os.path.basename(tmp.path)
    save_path = os.path.join(save_dir,file_name)
    if does_ERS_file_exist(file_name, save_dir):
        print('%s exists, skip downloading'%save_path)
        return

    # check free disk space
    free_GB = io_function.get_free_disk_space_GB(save_dir)
    total_wait_time = 0
    while free_GB < 50 and total_wait_time < 60 * 60 * 12:
        basic.outputlogMessage(' The free disk space (%.4f) is less than 50 GB, wait 60 seconds' % free_GB)
        time.sleep(60)
        total_wait_time += 60
        free_GB = io_function.get_free_disk_space_GB(save_dir)

    basic.outputlogMessage('start downloading %s'%url)
    web_driver.get(url)

    # wait until the file has been downloaded
    total_wait_time = 0
    while does_ERS_file_exist(file_name, save_dir) is False and total_wait_time < 60 * 60 * 12:
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
        time.sleep(0.5)   # wait 1 seconds

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
        # remove some records only cover a small portions (less than half) of the study area
        results = remove_record_only_cover_parts(results, aoi_wkt)

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


def test_search():
    extent_shp = os.path.expanduser('~/Data/Arctic/pan_Arctic/extent/SAR_coh_test_region/ALDs_Dawson_Yukon_Lipovsky_2004.shp')
    ROIs_wkt = shapefile_to_ROIs_wkt(extent_shp)
    aoi_wkt = ROIs_wkt[0]
    save_dir = 'esa_data'
    start = datetime(2004,3,1)
    stop = datetime(2004,11,1)
    platform = 'ERS'
    ext_base_name = io_function.get_name_no_ext(extent_shp)

    print(datetime.now(), 'Searching... ... ...')
    print(datetime.now(), 'Input search parameters:')
    print('roi_wkt:', aoi_wkt)
    print('save_dir, start_date, end_date:', save_dir, start, stop)
    # print('platform, product, flightDirection:', platform, product, orbit)

    results = query(aoi_wkt, start, stop, platform=platform,product='single-look-complex', contains=False, limit=500)
    results = remove_record_only_cover_parts(results, aoi_wkt)

    print(datetime.now(), 'Found %s results' % (len(results)))
    data_meta_path = os.path.join(save_dir, '%s_meta.json' % ext_base_name)

    save_query_results(results, data_meta_path)



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
    driver.implicitly_wait(30)
    time.sleep(30)

    # Step 4: Enter the username and password
    username_field = driver.find_element(By.NAME, 'usernameUserInput')
    password_field = driver.find_element(By.NAME, 'password')
    username_field.send_keys(username)
    password_field.send_keys(password)

    # Step 5: Submit the login form
    password_field.send_keys(Keys.RETURN)

    # Step 6: Wait for the page to load after submitting the form (use a timeout value that makes sense for your website)
    driver.implicitly_wait(30)

    # wait until the file has been downloaded
    total_wait_time = 0
    max_wait_time = 10 * 6* 10  # # max 10 minutes
    signed_str = 'Signed in as %s' % username
    while True:
        print(datetime.now(), 'waiting the login page')
        time.sleep(20)
        total_wait_time += 20
        if signed_str in driver.page_source is False and total_wait_time <  max_wait_time:
            break
    if total_wait_time < max_wait_time:
        print('Login successful!')
        return driver

    driver.quit()
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
    global b_rm_small_overlap
    b_rm_small_overlap = options.b_dont_rm_small_overlap

    if user_name is None or password is None:
        print('Get user name and password from the .netrc file')
        user_name, password = get_user_password_netrc()

    print(datetime.now(), 'download data from ESA, start_date: %s, end_date: %s, user: %s \nwill save to %s'%(start_date,end_date,user_name,save_dir))

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
    # test_search()
    # test_does_ERS_file_exist()
    # sys.exit(0)

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

    parser.add_option("", "--b_dont_rm_small_overlap",
                      action="store_false", dest="b_dont_rm_small_overlap",default=True,
                      help="if set, it will keep the images that only cover a small portion of the study area")


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