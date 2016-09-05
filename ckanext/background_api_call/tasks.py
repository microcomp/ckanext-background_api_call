from ckan.lib.celery_app import celery
import ckan.model as model
import ckan.plugins.toolkit as toolkit
import logging
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import ckan.logic as logic
import ckan.lib.base as base
from ckan.lib.base import config
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
import shutil

site_url = config.get("ckan.internal_site_url","http://127.0.0.1")

@celery.task(name = "background_api_call.__call_function")
def call_function(context, data_dict):
    context = json.loads(context)
    #logging.error(context)
    #logging.error("celery task started...")
    funct = ""
    try:
        funct = data_dict.get('function')
    except KeyError:
        funct = "" 
    #logging.error(funct)
    #toolkit.get_action(funct)(context, data_dict)
    #logging.error(context)

    api_url = urlparse.urljoin(site_url, 'api/3/action')
    url = api_url + '/'+data_dict['function']
    #logging.error(url)
    fil = data_dict.get("file", None)
    #logging.error( "file in data dict" + fil)
    data_dict.pop("function")
    #logging.error(data_dict)
    upload_files = []
    up = data_dict.pop("file")
    if fil != None:
        file_to_upload = open(up, 'rb+')
        upload_files = [('upload', file_to_upload)]
        
    #logging.error( "file in data dict")
    
    #data_dict.pop("upload")
    #logging.error(data_dict)
    response = requests.post(
        url,
        data=data_dict,
        headers={'Authorization': context['apikey']}, 
        files=upload_files)
        
    #logging.error(response.status_code)
        
    if response.status_code == 200:
        to = {}
        to["result"] = 'task complete...'
        to['response'] = json.loads(response.text)
        if funct == "resource_create" or funct == "resource_update":
            to["response"]["result"]["url"] = "/".join(to["response"]["result"]["url"].split("/")[:-1])+"/"+data_dict["file_name"]
    else:
        to = {}
        to["result"] = 'task failed...'
        to['response'] = json.loads(response.text)

    response2 = urlparse.urljoin(site_url, 'api/3/action')
    dd = {'to':json.dumps(to), 'id':data_dict['task_id']}
    response = requests.post(
        api_url + '/change_db_row',
        json.dumps(dd),
        headers={'Authorization': context['apikey'],
                 'Content-Type': 'application/json'}
    )
    if funct == "resource_create" or funct == "resource_update":
        requests.post(
            api_url + '/resource_update',
            json.dumps({"id":to["response"]["result"]["id"], "url":data_dict["file_name"]}),
            headers={'Authorization': context['apikey'],
                     'Content-Type': 'application/json'})
    
    import os
    os.remove(fil)
    dr = fil.split("/")
    dr = dr[:-1]
    os.rmdir("/".join(dr))
    return "done..."
