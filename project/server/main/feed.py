import datetime
import os
import pymongo
import requests
from urllib import parse
from urllib.parse import quote_plus
import json
from retry import retry
import random

from bs4 import BeautifulSoup
import math

from project.server.main.logger import get_logger
from project.server.main.utils_swift import upload_object, get_last_ref_date
from project.server.main.parse import parse_theses, get_idref_from_OS

logger = get_logger(__name__)

def get_ip():
    ip = requests.get('https://api.ipify.org').text
    return ip

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

    year_start = start_date_str_iso[0:4]
    year_end = end_date_str_iso[0:4]

    start = 0


    #url = "http://theses.fr/?q=&zone1=titreRAs&val1=&op1=AND&zone2=auteurs&val2=&op2=AND&zone3=etabSoutenances&val3=&op3=AND&zone4=sujDatePremiereInscription&val4a={}&val4b={}&start={}&format=xml"
    #logger.debug(url.format(start_date_str, end_date_str, start))
    #r = requests.get(url.format(start_date_str, end_date_str, start))
    #url = "https://theses.fr/?q=&start={}&format=xml"
    url = f'https://theses.fr/api/v1/theses/recherche/?q=*&debut=0&nombre=500&tri=dateAsc&filtres=%5Bdatefin%3D%22{year_end}%22~datedebut%3D%22{year_start}%22%5D'
    logger.debug(url)
    r = get_url(url).json()

    nb_res = r['totalHits']#soup.find('result', {'name': 'response'}).attrs['numfound']
    logger.debug("{} resultats entre {} et {}".format(nb_res, start_date_str_iso, end_date_str_iso ))
    #num_theses = get_num_these(soup)
    num_theses = [k['id'] for k in r['theses']]

    nb_pages_remaining = math.ceil(int(nb_res)/500)
    for p in range(1, nb_pages_remaining):
        logger.debug("page {} for entre {} et {}".format(p, start_date_str_iso, end_date_str_iso))
        #r = requests.get(url.format(start_date_str, end_date_str, p * 1000))
        debut = p * 500
        r = get_url(f'https://theses.fr/api/v1/theses/recherche/?q=*&debut={debut}&nombre=500&tri=dateAsc&filtres=%5Bdatefin%3D%22{year_end}%22~datedebut%3D%22{year_start}%22%5D').json()
        #soup = BeautifulSoup(r.text, 'lxml')
        #num_theses += get_num_these(soup)
        num_theses += [k['id'] for k in r['theses']]
        
    return num_theses


@retry(delay=10, tries=10)
def get_url(url):
    #logger.debug(url)
    return requests.get(url)

crawler_url = "http://crawler:5001"
@retry(delay=10, tries=10)
def get_url_from_ip(url):
    #return get_url_bright(url)
    res = requests.post(f'{crawler_url}/simple_crawl', json={'url': url}).json()
    if res.get('status'):
        return res['text']


@retry(delay=10, tries=10)
def get_url_bright(url):
    PASSWORD = os.getenv('BRIGHT_PASSWORD')
    USER = os.getenv('BRIGHT_USERNAME')
    PORT = os.getenv('BRIGHT_PORT')
    rdm = str(int(100000*random.random()))
    os.system(f'rm -rf current_{rdm}.html')
    cmd = f'curl --proxy brd.superproxy.io:{PORT} --proxy-user {USER}:{PASSWORD} -k {url} -o current_{rdm}.html'
    os.system(cmd)
    source = open(f'current_{rdm}.html', 'r').read()
    return source

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

@retry(delay=60, tries=5)
def download_these_notice(these_id):
    res = {'id': these_id}
    #url_tefudoc = "https://www.theses.fr/{}.tefudoc".format(these_id)
    #url_xml = "https://www.theses.fr/{}.xml".format(these_id)
    url_tefudoc = f"https://theses.fr/api/v1/export/tefudoc/{these_id}"
    url_xml= f"https://theses.fr/api/v1/export/xml/{these_id}"
    if these_id[0:1] != 's':
        #r_tefudoc = get_url(url_tefudoc).text
        r_tefudoc = get_url_from_ip(url_tefudoc)
        if r_tefudoc[0:5] == "<?xml":
            res['tefudoc'] = r_tefudoc
    #r_xml = get_url(url_xml).text
    r_xml = get_url_from_ip(url_xml)
    if r_xml[0:5] == "<?xml":
        res['xml'] = r_xml
    return res

def harvest_and_insert_year(collection_name, year_start, year_end, referentiel):
    
    year_start_end = 'all_years'
    if year_start and year_end:
        year_start_end = f'{year_start}_{year_end}'

    start_date = datetime.datetime(year_start,1,1)
    end_date = datetime.datetime(year_end + 1,1,1) + datetime.timedelta(days = -1)
    nnt_filename = f'all_nnts_{year_start_end}.json'

    try:
        json.load(open(nnt_filename, 'r'))
    except:
        all_num_theses = get_num_these_between_dates(start_date, end_date)
    json.dump(all_num_theses, open(nnt_filename, 'w'))


    # todo save by chunk
    chunk_index = 0
    data = []
    MAX_DATA_SIZE = 25000
    nb_theses = len(all_num_theses)
    logger.debug(f'{nb_theses} theses to download and parse')
    logger.debug(f'current IP = {get_ip()}')
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
