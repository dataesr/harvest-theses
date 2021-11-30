import gzip
import os
import pandas as pd
import swiftclient

from io import BytesIO, TextIOWrapper
from retry import retry

from project.server.main.logger import get_logger

logger = get_logger(__name__)
SWIFT_SIZE = 10000
key = os.getenv('OS_PASSWORD')
project_name = os.getenv('OS_PROJECT_NAME')
project_id = os.getenv('OS_TENANT_ID')
tenant_name = os.getenv('OS_TENANT_NAME')
username = os.getenv('OS_USERNAME')
user = f'{tenant_name}:{username}'
init_cmd = f"swift --os-auth-url https://auth.cloud.ovh.net/v3 --auth-version 3 \
      --key {key}\
      --user {user} \
      --os-user-domain-name Default \
      --os-project-domain-name Default \
      --os-project-id {project_id} \
      --os-project-name {project_name} \
      --os-region-name GRA"
conn = None


def get_connection() -> swiftclient.Connection:
    global conn
    if conn is None:
        conn = swiftclient.Connection(
            authurl='https://auth.cloud.ovh.net/v3',
            user=user,
            key=key,
            os_options={
                'user_domain_name': 'Default',
                'project_domain_name': 'Default',
                'project_id': project_id,
                'project_name': project_name,
                'region_name': 'GRA'
            },
            auth_version='3'
        )
    return conn


@retry(delay=2, tries=50)
def upload_object(container: str, source: str, target:str) -> str:
    logger.debug(f'Uploading {source} in {container} as {target}')
    cmd = init_cmd + f' upload {container} {source} --object-name {target}' \
                     f' --segment-size 1048576000 --segment-threads 100'
    os.system(cmd)
    return f'https://storage.gra.cloud.ovh.net/v1/AUTH_{project_id}/{container}/{target}'


@retry(delay=2, tries=50)
def download_object(container: str, filename: str, out: str) -> None:
    logger.debug(f'Downloading {filename} from {container} to {out}')
    cmd = init_cmd + f' download {container} {filename} -o {out}'
    os.system(cmd)
