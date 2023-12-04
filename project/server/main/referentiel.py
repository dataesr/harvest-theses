import datetime
import urllib
import os
import json
import re
import pickle
import requests
from bs4 import BeautifulSoup
from dateutil import parser
from traceback import format_exc
from retry import retry
from project.server.main.utils_swift import upload_object, download_object
from project.server.main.logger import get_logger

logger = get_logger(__name__)

USERNAME = os.getenv('BRIGHT_USERNAME')
PASSWORD = os.getenv('BRIGHT_PASSWORD')
PORT = os.getenv('BRIGHT_PORT')

@retry(delay=60, tries=5)
def get_idref_list_in_referentiel():
    NB_ROWS = 10000
    start = 0

    idref_to_download = []
    keep_going = True

    while keep_going:
        url = f'https://www.idref.fr/Sru/Solr?q=recordtype_z:b&version=2.2&start={start}&rows={NB_ROWS}&wt=json&fl=ppn_z'
        res = requests.get(url).json()['response']
        idref_to_download += [p['ppn_z'] for p in res['docs']]
        keep_going = len(res['docs']) > 0
        start += NB_ROWS
        logger.debug(f"{len(idref_to_download)} sur {res['numFound']}")
    return idref_to_download

def download_referentiel_notice2(idref):
    url = f'https://www.idref.fr/{idref}.xml'
    opener = urllib.request.build_opener(
    urllib.request.ProxyHandler(
            {'http': f'{USERNAME}:{PASSWORD}@zproxy.lum-superproxy.io:{PORT}',
            'https': f'{USERNAME}:{PASSWORD}@zproxy.lum-superproxy.io:{PORT}'}))
    #print(f'BRIGHT url = {url}', flush=True)
    req = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
    try:
        source = urllib.request.urlopen(req).read()
    except:
        logger.debug(f'crawl error for {url}')
        return None
    #source = opener.open(url).read()
    if type(source) == bytes:
        try:
            source = source.decode("utf-8")
        except:
            source = str(source)
    if source[0:5] == '<?xml':
        return source
    return None


def get_referentiel(collection_name):
    idref_to_download = get_idref_list_in_referentiel()
    
    chunk_index = 0
    data_notice, data_parsed = [], []
    MAX_DATA_SIZE = 5000
    #MAX_DATA_SIZE = 100 #fast
    nb_idref = len(idref_to_download)
    logger.debug(f'{nb_idref} idref struct to download and parse')
    for ix, idref in enumerate(idref_to_download):
        if ix % 10 == 0:
            logger.debug(f'idref struct {ix}')
        try:
            notice = download_referentiel_notice(idref)
            elt_notice = {'idref': idref, 'notice': notice}
            data_notice.append(elt_notice)
            elt_parsed = parse_idref(idref, notice)
            data_parsed.append(elt_parsed)
        except:
            logger.debug(f'error in downloading notice for idref {idref}')
            continue
        if (len(data_notice) > MAX_DATA_SIZE) or (ix == nb_idref - 1):
            if data_notice:
                source = f'idref_struct_raw_{chunk_index}.json'
                target = f'{collection_name}/{source}'
                save_data_referentiel(data_notice, source, target)
                data_notice = []
                chunk_index += 1
    return data_parsed

def save_data_referentiel(data, source, target):
    current_file = source
    json.dump(data, open(current_file, 'w'))
    os.system(f'gzip {current_file}')
    upload_object('theses', f'{source}.gz', f'{target}.gz')
    os.system(f'rm -rf {source}.gz')

def parse_idref(idref, notice):
    soup = BeautifulSoup(notice, 'lxml')
    elt = {}
    idref_elt = soup.find('controlfield', {"tag": "001"})
    try:
        assert(idref_elt.text == idref)
    except Exception as e:
        print("idref not matching in notice")

    elt['idref'] = idref
    alias_idref = []
    aliases = []

    name_elt = soup.find("datafield", {"tag": "210"})
    if name_elt and name_elt.find("subfield", {'code': 'a'}):
        elt['name'] = name_elt.find("subfield", {'code': 'a'}).text

    for f in soup.find_all("datafield", {"tag": "410"}) + soup.find_all("datafield", {"tag": "911"}):
        if f.find("subfield", {'code': 'a'}):
            aliases.append(f.find("subfield", {'code': 'a'}).text)
    elt['aliases'] = list(set(aliases))

    addresses, websites = [], []
    comments = []
    for f in soup.find_all("datafield", {"tag": "340"}):
        if f.find("subfield", {'code': 'a'}):
            txt = f.find("subfield", {'code': 'a'}).text
            if 'adresse' in txt.lower() and len(txt.split(':'))>1:
                addresses.append(':'.join(txt.split(':')[1:]).strip())
            elif 'site internet' in txt.lower() and len(txt.split(':'))>1:
                websites.append(':'.join(txt.split(':')[1:]).strip())
            else:
                comments.append(txt.strip())

    if addresses:
        elt['addresses'] = addresses
    if websites:
        elt['websites'] = websites
    if comments:
        elt['comments'] = comments

    for f in soup.find_all("datafield", {"tag": "010"}):
        if f.find("subfield", {'code': 2}) and f.find("subfield", {'code': 2}).text=='ISNI':
            if f.find("subfield", {'code': 'a'}):
                elt['ISNI'] = f.find("subfield", {'code': 'a'}).text
    
    for f in soup.find_all("datafield", {"tag": "033"}):
        if f.find("subfield", {'code': 2}) and f.find("subfield", {'code': 2}).text=='BNF':
            if f.find("subfield", {'code': 'a'}):
                elt['BNF'] = f.find("subfield", {'code': 'a'}).text

    for f in soup.find_all("datafield", {"tag": "035"}):
        if f.find("subfield", {'code': 9}) and f.find("subfield", {'code': 9}).text=='sudoc':
            if f.find("subfield", {'code': 'a'}):
                alias_idref.append(f.find("subfield", {'code': 'a'}).text)


        if f.find("subfield", {'code': 2}) and f.find("subfield", {'code': 2}).text=='HAL':
            if f.find("subfield", {'code': 'a'}):
                elt['docid'] = f.find("subfield", {'code': 'a'}).text

        if f.find("subfield", {'code': 2}) and f.find("subfield", {'code': 2}).text=='RNSR':
            if f.find("subfield", {'code': 'a'}):
                elt['rnsr'] = f.find("subfield", {'code': 'a'}).text

        if f.find("subfield", {'code': 2}) and f.find("subfield", {'code': 2}).text=='VIAF':
            if f.find("subfield", {'code': 'a'}):
                elt['viaf'] = f.find("subfield", {'code': 'a'}).text

    elt['alias_idref'] = alias_idref
    return elt


@retry(delay=60, tries=5)
def download_referentiel_notice(idref):
    r = requests.get(f'https://www.idref.fr/{idref}.xml')
    if r.text[0:5] == '<?xml':
        return r.text
    return None

def create_idref_map(data):
    idref_map = {}
    parsed_data = []
    for d in data:
        idrefs = [d['idref']]
        parsed_elt = d
        idrefs += parsed_elt.get('alias_idref', [])
        idrefs = list(set(idrefs))
        parsed_data.append(parsed_elt)
        for idref in idrefs:
            idref_map[str(idref)] = parsed_elt
    logger.debug(f'{len(data)} elts and {len(idref_map)} idrefs (structure) in map')
    return parsed_data, idref_map

def harvest_and_save_idref(collection_name):
    # raw data
    data = get_referentiel(collection_name)
    
    #parsed data
    parsed_data, idref_map = create_idref_map(data)
    
    source = f'idref_struct.json'
    target = f'{collection_name}/{source}'
    save_data_referentiel(parsed_data, source, target)
    
    # idref mapping
    source = f'idref_struct_dict.json'
    target = f'{collection_name}/{source}'
    save_data_referentiel(idref_map, source, target)
