import datetime
import os
import json
import re
import pickle
from bs4 import BeautifulSoup
from dateutil import parser
from traceback import format_exc
from tokenizers import normalizers
from tokenizers.normalizers import NFD, StripAccents, Lowercase, BertNormalizer, Sequence, Strip
from tokenizers import pre_tokenizers
from tokenizers.pre_tokenizers import Whitespace
from project.server.main.utils_swift import upload_object, download_object
from project.server.main.logger import get_logger

normalizer = Sequence([BertNormalizer(clean_text=True,
        handle_chinese_chars=True,
        strip_accents=True,
        lowercase=True), Strip()])
pre_tokenizer = pre_tokenizers.Sequence([Whitespace()])

logger = get_logger(__name__)

dewey = pickle.load(open("project/server/main/dewey.pkl", 'rb'))

def normalize(x, min_length = 0):
    normalized = normalizer.normalize_str(x)
    normalized = normalized.replace('\n', ' ')
    normalized = re.sub(' +', ' ', normalized)
    return " ".join([e[0] for e in pre_tokenizer.pre_tokenize_str(normalized) if len(e[0]) > min_length])

def get_millesime(x: str) -> str:
    try:
        if x[0:4] < '2021':
            return x[0:4]
        month = int(x[4:6])
        if 1 <= month <= 3:
            return x[0:4] + 'Q1'
        if 4 <= month <= 6:
            return x[0:4] + 'Q2'
        if 7 <= month <= 9:
            return x[0:4] + 'Q3'
        if 10 <= month <= 12:
            return x[0:4] + 'Q4'
        return 'unk'
    except:
        return x

def get_dewey(dewey_code):
    thematics = []
    thematic = {'code': dewey_code, 'reference': 'dewey'}
    if dewey_code in dewey:
        thematic['label'] = dewey[dewey_code]['en']
        thematic['label_fr'] = dewey[dewey_code]['fr']
    thematics.append(thematic)
    if dewey_code[-1] != '0':
        dewey_parent = get_dewey(dewey_code[0:2]+"0")
        thematics += dewey_parent
    if dewey_code[-2:] != '00':
        dewey_parent = get_dewey(dewey_code[0:1]+"00")
        thematics += dewey_parent
    return thematics

def get_person(xml_object):
    try:
        author_object = xml_object.find('foaf:person')
        [last_name, first_name] = author_object.find('foaf:name').text.split(',')
        full_name = first_name + ' ' + last_name
        person = {'last_name': last_name.strip(), 'first_name': first_name.strip(), 'full_name': full_name.strip()}
    except:
        person = {}
    try:
        person_id = author_object.attrs['rdf:about'].replace('http://www.idref.fr/','').replace('/id', '')
        person["idref"] = person_id
    except:
        pass
    return person

def get_person2(author_object):
    person = {}
    if author_object is None:
        return person
    full_name = ''
    if author_object.find('tef:prenom'):
        person['first_name'] = author_object.find('tef:prenom').text.strip()
        full_name = person['first_name']
    if author_object.find('tef:nom'):
        person['last_name'] = author_object.find('tef:nom').text.strip()
        full_name += f" {person['last_name']}"
    
    if full_name:
        person['full_name'] = full_name.strip()

    try:
        person_id = author_object.find('tef:autoriteexterne', {'autoritesource' :'Sudoc'}).text
        person["idref"] = person_id
    except:
        pass
    return person


def get_idref_from_OS(collection_name):
    target_file = f'idref_struct_{collection_name}_dict.json'
    os.system(f'rm -rf {target_file}.gz')
    os.system(f'rm -rf {target_file}')
    download_object(container='theses', filename=f'{collection_name}/idref_struct_dict.json.gz', out=f'{target_file}.gz')
    os.system(f'gunzip {target_file}.gz')
    return json.load(open(target_file, 'r'))

def parse_theses(notice, referentiel, snapshot_date):
    try:
        return parse_theses_xml(notice, referentiel, snapshot_date)
    except Exception as e:
        logger.debug(f"error in parsing these {notice['id']}")
        logger.debug(f'{format_exc()}')
        return parse_theses_xml(notice, referentiel, snapshot_date)
        return {}

def get_parsed_date(soup, date_balise):
    date_elt = soup.find(date_balise)
    if date_elt:
        datestr = date_elt.text
        if len(datestr) == 4:
            datestr = datestr + "-01-01"
        if len(datestr) > 10:
            datestr = datestr[0:10]
        parsed_date = datetime.datetime.strptime(
                    datestr, "%Y-%m-%d"
                )
        return parsed_date.isoformat()
    return None

def parse_theses_xml(notice, referentiel, snapshot_date):
    res = {}
    res['sources'] = ['theses']
    external_ids = []
    if isinstance(notice['id'], str):
        external_ids.append({'id_type': 'nnt_id', 'id_value': notice.get('id')})
        res['nnt_id'] = notice.get('id')
    if external_ids:
        res['external_ids'] = external_ids
    
    res['genre'] = 'thesis'

    soup = None
    soup_tefudoc = None
    soup_xml = None

    if 'tefudoc' in notice:
        soup_tefudoc = BeautifulSoup(notice['tefudoc'], 'lxml')
    if 'xml' in notice:
        soup_xml = BeautifulSoup(notice['xml'], 'lxml')
   
    if soup_tefudoc:
        soup = soup_tefudoc
    elif soup_xml:
        soup = soup_xml
    else:
        return res

    title_elt = soup.find('dc:title')
    if title_elt is None:
        return res

    res['title'] = soup.find('dc:title').text

    abstracts = []
    if isinstance(soup.find_all('dcterms:abstract'), list):
        for abstract in soup.find_all('dcterms:abstract'):
            lang = None
            if 'xml:lang' in abstract.attrs:
                lang = abstract.attrs['xml:lang']
            if isinstance(abstract.text, str):
                abstracts.append({'lang': lang, 'abstract': abstract.text.strip() })
    if abstracts:
        res['abstract'] = abstracts

    classifications = []
    keywords = []
    for topic in soup.find_all('dc:subject') + soup.find_all('dcterms:subject'):
        lang = None

        if 'xsi:type' in topic.attrs and topic.attrs['xsi:type'] == "dcterms:DDC":
            dewey_code = topic.text
            if dewey_code and len(dewey_code) == 3:
                classifications += get_dewey(dewey_code)

        elif 'rdf:resource' in topic.attrs and 'http://dewey.info/' in topic.attrs['rdf:resource']:
            dewey_code = topic.attrs['rdf:resource'].replace("http://dewey.info/class/","")[0:3]
            if dewey_code and len(dewey_code) == 3:
                classifications += get_dewey(dewey_code)
        
        else:
            if 'xml:lang' in topic.attrs:
                lang = topic.attrs['xml:lang']
            keywords.append({'lang': lang, 'keyword': topic.text.strip()})
    
    for sub_elt in soup.find_all('tef:oaisetspec'):
        if 'ddc' in sub_elt.text:
            classifications += get_dewey(sub_elt.text.replace('ddc:', ''))

    for v in soup.find_all('tef:vedetterameaunomcommun')+soup.find_all('tef:vedetterameauauteurtitre'):
        elt_entree = v.find('tef:elementdentree')
        if elt_entree is None:
            continue
        reference = elt_entree.attrs.get('autoritesource', '').lower()
        code =  elt_entree.attrs.get('autoriteexterne', '').lower()
        label = elt_entree.text
        label_fr = elt_entree.text
        thematic = {'reference': reference, 'label': label, 'label_fr': label, 'code':code}
        classifications.append(thematic)

    try:
        discipline = soup.find('tef:thesis.degree.discipline').text
        thematic_degree = {'reference': 'degree discipline', 'label_fr': discipline }
        classifications.append(thematic_degree)
    except:
        pass
    
    if keywords:
        res['keywords'] = keywords

    if classifications:
        classifications_unique = []
        for t in classifications:
            if t not in classifications_unique:
                classifications_unique.append(t)
        res['classifications'] = classifications_unique

    res['is_defended'] = False
    defense_date = get_parsed_date(soup, 'dcterms:dateaccepted')
    if defense_date:
        res['defense_date'] = defense_date
        res['year'] = defense_date[0:4]
        res['is_defended'] = True
    
    start_date = get_parsed_date(soup, 'dcterms:created')
    if start_date:
        res['start_date'] = start_date

    is_oa = False
    oa_locations = []
    if soup.find('dc:identifier') and 'document' in soup.find('dc:identifier').text:
        is_oa = True
        oa_url = soup.find('dc:identifier').text

    elif soup_xml.find('dc:identifier') and 'document' in soup_xml.find('dc:identifier').text:
        is_oa = True
        oa_url = soup_xml.find('dc:identifier').text
    
    #embargo confidentialite
    
    sorties = soup.find('sorties')
    if sorties and sorties.find('diffusion'):
        diffusion_attrs = sorties.find('diffusion').attrs
        res.update(diffusion_attrs)
    diffusion_types = []
    if is_oa is True:
        diffusion_types.append('open')
    if is_oa is False:
        if res.get('embargofin'):
            diffusion_types.append('embargo')
        if res.get('confidentialitefin'):
            diffusion_types.append('confidential')
        if len(diffusion_types) == 0:
            diffusion_types.append('restricted')
    diffusion_types.sort()

    res['oa_details'] = {}
    observation_date = get_millesime(snapshot_date)
    res['oa_details'][observation_date] = {'is_oa': is_oa, 'observation_date': observation_date, 'snapshot_date': snapshot_date, 'diffusion_type': ';'.join(diffusion_types)}
    if is_oa:
        res['oa_details'][observation_date]['oa_host_type'] = 'repository'
        res['oa_details'][observation_date]['oa_colors'] = ['green']
        res['oa_details'][observation_date]['oa_colors_with_priority_to_publisher'] = ['green_only']
        res['oa_details'][observation_date]['repositories'] = ['theses.fr']
        res['oa_details'][observation_date]['oa_locations'] = [{'url': oa_url, 'repository_normalized': 'theses.fr', 'host_type': 'repository'}]
    else:
        res['oa_details'][observation_date]['oa_host_type'] = 'closed'
        res['oa_details'][observation_date]['oa_colors'] = ['closed']
        res['oa_details'][observation_date]['oa_colors_with_priority_to_publisher'] = ['closed']

    for mysoup in [soup, soup_xml]:
        for dci in mysoup.find_all('dc:identifier', {'xsi:type': "dcterms:URI"}):
            link = dci.text
            if 'archives-ouvertes' in link:
                hal_id = link.split('/')[-1]
                if len(hal_id) > 2 and res.get('hal_id') is None:
                    res['hal_id'] = hal_id.lower()
                    external_ids.append({'id_type': 'hal_id', 'id_value': res['hal_id']})
                    break

    affiliations = []

    for org in soup.find_all('tef:ecoledoctorale') + soup.find_all('tef:partenairerecherche') + soup.find_all('tef:thesis.degree.grantor'):
        org_name = org.find('tef:nom').text
        current_affiliation = {'name': org_name}
        if org_name is None:
            continue
        try:
            id_org = org.find('tef:autoriteexterne', {'autoritesource': "Sudoc"}).text
        except:
            id_org = None
        
        if id_org:
            current_affiliation['idref'] = id_org
            if id_org in referentiel:
                current_affiliation.update(referentiel[id_org])
            else:
                logger.debug("IDREF struct missing : {}".format(id_org))
            
        affiliations.append(current_affiliation)

    author = {}
    if soup_tefudoc:
        author = get_person2(soup.find('tef:auteur'))
    elif soup_xml:
        author = get_person(soup.find('marcrel:dis'))

    if affiliations:
        res['affiliations'] = affiliations
        if author:
            author['affiliations'] = affiliations

    authors = []
    if author:
        author['role'] = 'author'
        authors = [author]

    for role in ['directeurthese', 'presidentjury', 'membrejury', 'rapporteur']:
        for elt in soup.find_all('tef:'+role):
            contributor = get_person2(elt)
            if contributor:
                contributor['role'] = role
                authors.append(contributor)
    if soup_tefudoc is None and soup_xml:
        for p in soup.find_all('marcrel:ths'):
            contributor = get_person(p)
            contributor['role'] = 'directeurthese'
            authors.append(contributor)

    if authors:
        res['authors'] = authors

    #for author in authors:
    #    if 'id' in author:
    #        exists_or_download_author(author.copy())

    ## title - first author
    title_first_author = ""
    if res.get('title'):
        title_first_author += normalize(res.get('title'), 1).strip()
    if isinstance(res.get('authors'), list) and len(res['authors']) > 0:
        if res['authors'][0].get('full_name'):
            title_first_author += ';'+normalize(res['authors'][0].get('full_name'), 1)
    if title_first_author:
        res['title_first_author'] = title_first_author
    return res
