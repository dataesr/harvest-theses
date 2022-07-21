import datetime
import os
import pymongo
import requests
from urllib import parse
from urllib.parse import quote_plus
import json
from retry import retry

from bs4 import BeautifulSoup
import math

from project.server.main.logger import get_logger
from project.server.main.utils_swift import upload_object
from project.server.main.parse import parse_theses, get_idref_from_OS
from project.server.main.referentiel import harvest_and_save_idref

logger = get_logger(__name__)

def get_num_these(soup):
    num_theses = []
    for d in soup.find_all('doc'):
        num_theses.append(d.find('str', {'name': 'num'}).text)
    return num_theses

@retry(delay=60, tries=5)
def get_num_these_between_dates(start_date, end_date):
    start_date_str = start_date.strftime("%d/%m/%Y")
    end_date_str = end_date.strftime("%d/%m/%Y")

    start_date_str_iso = start_date.strftime("%Y%m%d")
    end_date_str_iso = end_date.strftime("%Y%m%d")

    start = 0


    #url = "http://theses.fr/?q=&zone1=titreRAs&val1=&op1=AND&zone2=auteurs&val2=&op2=AND&zone3=etabSoutenances&val3=&op3=AND&zone4=sujDatePremiereInscription&val4a={}&val4b={}&start={}&format=xml"
    #logger.debug(url.format(start_date_str, end_date_str, start))
    #r = requests.get(url.format(start_date_str, end_date_str, start))
    url = "http://theses.fr/?q=&start={}&format=xml"

    r = requests.get(url.format(start))

    soup = BeautifulSoup(r.text, 'lxml')

    nb_res = soup.find('result', {'name': 'response'}).attrs['numfound']
    logger.debug("{} resultats entre {} et {}".format(nb_res, start_date_str_iso, end_date_str_iso ))
    num_theses = get_num_these(soup)

    nb_pages_remaining = math.ceil(int(nb_res)/1000)
    for p in range(1, nb_pages_remaining):
        logger.debug("page {} for entre {} et {}".format(p, start_date_str_iso, end_date_str_iso))
        #r = requests.get(url.format(start_date_str, end_date_str, p * 1000))
        r = requests.get(url.format(p * 1000))
        soup = BeautifulSoup(r.text, 'lxml')
        num_theses += get_num_these(soup)
        
    return num_theses


def save_data(data, collection_name, year_start, year_end, chunk_index, referentiel):
    logger.debug(f'save_data theses {collection_name} {chunk_index}')
    year_start_end = 'all_years'
    if year_start and year_end:
        year_start_end = f'{year_start}_{year_end}'
    # 1. save raw data to OS
    current_file = f'theses_{year_start_end}_{chunk_index}.json'
    json.dump(data, open(current_file, 'w'))
    os.system(f'gzip {current_file}')
    upload_object('theses', f'{current_file}.gz', f'{collection_name}/raw/{current_file}.gz')
    os.system(f'rm -rf {current_file}.gz')

    # 2.transform data and save in mongo
    current_file_parsed = f'theses_parsed_{year_start_end}_{chunk_index}.json'
    data_parsed = [parse_theses(e, referentiel, collection_name) for e in data]
    json.dump(data_parsed, open(current_file_parsed, 'w'))
    # insert_data(collection_name, current_file_parsed)
    os.system(f'gzip {current_file_parsed}')
    upload_object('theses', f'{current_file_parsed}.gz', f'{collection_name}/parsed/{current_file_parsed}.gz')
    os.system(f'rm -rf {current_file_parsed}.gz')

def harvest_and_insert(collection_name, harvest_referentiel):
    # 1. save aurehal structures
    if harvest_referentiel:
        harvest_and_save_idref(collection_name)
    referentiel = get_idref_from_OS(collection_name)

    # 2. drop mongo 
    #logger.debug(f'dropping {collection_name} collection before insertion')
    #myclient = pymongo.MongoClient('mongodb://mongo:27017/')
    #myclient['theses'][collection_name].drop()

    # 3. save publications
    year_start = None
    year_end = None
    if year_start is None:
        year_start = 1990
    if year_end is None:
        year_end = datetime.date.today().year
    harvest_and_insert_one_year(collection_name, year_start, year_end, referentiel)

@retry(delay=60, tries=5)
def download_these_notice(these_id):

    res = {'id': these_id}
    r_tefudoc = requests.get("http://www.theses.fr/{}.tefudoc".format(these_id))
    r_xml = requests.get("http://www.theses.fr/{}.xml".format(these_id))

    if r_tefudoc.text[0:5] == "<?xml":
        res['tefudoc'] = r_tefudoc.text

    if r_xml.text[0:5] == "<?xml":
        res['xml'] = r_xml.text

    return res

def harvest_and_insert_one_year(collection_name, year_start, year_end, referentiel):
    
    year_start_end = 'all_years'
    if year_start and year_end:
        year_start_end = f'{year_start}_{year_end}'

    start_date = datetime.datetime(year_start,1,1)
    end_date = datetime.datetime(year_end + 1,1,1) + datetime.timedelta(days = -1)

    all_num_theses = get_num_these_between_dates(start_date, end_date)

    # todo save by chunk
    chunk_index = 0
    data = []
    MAX_DATA_SIZE = 25000
    nb_theses = len(all_num_theses)
    logger.debug(f'{nb_theses} theses to download and parse')
    for ix, nnt in enumerate(all_num_theses):
        if ix % 100 == 0:
            logger.debug(f'theses {year_start_end} {ix}')
        res = download_these_notice(nnt)
        data.append(res)
        
        if (len(data) > MAX_DATA_SIZE) or (ix == nb_theses - 1):
            if data:
                save_data(data, collection_name, year_start, year_end, chunk_index, referentiel)
                data = []
                chunk_index += 1


def insert_data(collection_name, output_file):
    myclient = pymongo.MongoClient('mongodb://mongo:27017/')
    mydb = myclient['theses']
    
    ## mongo start
    start = datetime.datetime.now()
    mongoimport = f"mongoimport --numInsertionWorkers 2 --uri mongodb://mongo:27017/theses --file {output_file}" \
                  f" --collection {collection_name} --jsonArray"
    logger.debug(f'Mongoimport {output_file} start at {start}')
    logger.debug(f'{mongoimport}')
    os.system(mongoimport)
    logger.debug(f'Checking indexes on collection {collection_name}')
    mycol = mydb[collection_name]
    #mycol.create_index('docid')
    end = datetime.datetime.now()
    delta = end - start
    logger.debug(f'Mongoimport done in {delta}')
    ## mongo done
