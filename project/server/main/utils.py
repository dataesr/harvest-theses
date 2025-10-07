import requests
from retry import retry

@retry(delay=10, tries=10)
def get_url_from_ip(url):
    proxies = {
        'http': 'http://dataesr:proxyovh@51.210.36.87:3128',
        'https': 'http://dataesr:proxyovh@51.210.36.87:3128'
    }
    res = requests.get(url, proxies=proxies)
    return res

@retry(delay=10, tries=10)
def get_url(url):
    #logger.debug(url)
    return requests.get(url)

crawler_url = "http://crawler:5001"
@retry(delay=10, tries=10)
def get_url_from_ip_crawler(url):
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

