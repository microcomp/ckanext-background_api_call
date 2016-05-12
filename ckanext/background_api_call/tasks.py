from ckan.lib.celery_app import celery
import ckan.model as model
import ckan.plugins.toolkit as toolkit
import logging
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import ckan.logic as logic
import ckan.lib.base as base
import ckan.lib.helpers as h
import ckan.lib.navl.dictization_functions as df
import ckan.plugins as p
from ckan.common import _, c

import api_calls
import json

import datetime
import mimetypes
import requests
import urlparse

@celery.task(name = "background_api_call.__call_function")
def call_function(context, data_dict):
    context = json.loads(context)
    logging.error(context)
    logging.error("celery task started...")
    funct = ""
    try:
    	funct = data_dict.get('function')
    except KeyError:
    	funct = "" 
    logging.error(funct)
    #toolkit.get_action(funct)(context, data_dict)
    logging.error(context)
    api_url = urlparse.urljoin(context['site_url'], 'api/action')
    response = requests.post(
        api_url + '/'+data_dict['function'],
        json.dumps(data_dict),
        headers={'Authorization': context['apikey'],
                 'Content-Type': 'application/json'}
    )
    logging.error(response.status_code)
    if response.status_code == 200:
    	to = "task complete..."
    else:
    	to = "task failed..."
    response2 = urlparse.urljoin(context['site_url'], 'api/action')
    dd = {'to':to, 'id':data_dict['task_id']}
    response = requests.post(
        api_url + '/change_db_row',
        json.dumps(dd),
        headers={'Authorization': context['apikey'],
                 'Content-Type': 'application/json'}
    )
    return "done..."