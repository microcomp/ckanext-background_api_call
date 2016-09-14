from ckan.lib.celery_app import celery
import logging

from ckan.lib.base import config
from ckan.common import _, c

import json

import requests
import urlparse


site_url = config.get("ckan.internal_site_url","http://127.0.0.1")

@celery.task(name = "background_api_call.__call_function")
def call_function(context, data_dict):
    context = json.loads(context)
    api_url = urlparse.urljoin(site_url, 'api/3/action')
    requests.post(
        api_url + '/change_db_row',
        json.dumps( {'to':json.dumps({ 'result':'task started...'}), 'id':data_dict['task_id']} ),
        headers={'Authorization': context['apikey'],
                 'Content-Type': 'application/json'}
    )
    
    logging.error(context)
    logging.error("celery task started...")
    funct = ""
    try:
        funct = data_dict.get('function')
    except KeyError:
        funct = "" 

    
    url = api_url + '/'+data_dict['function']
    fil = data_dict.get("file", None)
    data_dict.pop("function")
    upload_files = []
    up = data_dict.pop("file")
    if fil != None:
        file_to_upload = open(up, 'rb+')
        upload_files = [('upload', file_to_upload)]
        
    response = requests.post(
        url,
        data=data_dict,
        headers={'Authorization': context['apikey']}, 
        files=upload_files)
        
        
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

    dd = {'to':json.dumps(to), 'id':data_dict['task_id']}
    requests.post(
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
