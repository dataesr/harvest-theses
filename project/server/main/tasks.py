import time
import datetime
import os
import requests

from project.server.main.logger import get_logger

logger = get_logger(__name__)

#def create_task_harvest(arg):
#    collection_name = arg.get('collection_name')
#    harvest_referentiel = arg.get('harvest_referentiel', True)
#    if collection_name:
#        harvest_and_insert(collection_name, harvest_referentiel)

