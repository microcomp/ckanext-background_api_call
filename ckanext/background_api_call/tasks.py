from ckan.lib.celery_app import celery
import logging

from ckan.lib.base import config
from ckan.common import _, c

import json

import requests
import urlparse


site_url = config.get("ckan.internal_site_url","http://127.0.0.1")

@celery.task(name = "background_api_call.__call_function", default_retry_delay = 10, max_retries=3)
def call_function(context, data_dict):
    context = json.loads(context)
    api_url = urlparse.urljoin(site_url, 'api/3/action')
    logging.debug("background_api_call: ")
    logging.debug(data_dict)
    requests.post(
        api_url + '/change_db_row',
        data = json.dumps({'to':json.dumps({ 'result':'task started...'}), 'id':data_dict['task_id']}) ,
        headers={'Authorization': context['apikey'],
                 'content-type': 'application/json'}
    )
    
    logging.debug("background_api_call: ")
    logging.debug(context)
    logging.debug("background_api_call: "+"celery task started...")
    funct = ""
    try:
        funct = data_dict.get('function')
    except KeyError:
        funct = "" 

    for i in data_dict.keys():
        if type(data_dict[i]) == unicode or type(data_dict[i]) == str:
            data_dict[i] = data_dict[i].encode('utf-8')
    

    url = api_url + '/'+data_dict['function']
    fil = data_dict.get("file", None)
    data_dict.pop("function")
    upload_files = []
    response = None
    try:
        up = data_dict.pop("file")
        file_to_upload = open(up, 'rb+')
        upload_files = [('upload', file_to_upload)]
        logging.debug("background_api_call: "+"upload...")
        response = requests.post(url,data=data_dict,headers={'Authorization': context['apikey']}, files=upload_files)
    except KeyError:
        logging.debug("background_api_call: "+"no upload...")
        logging.debug("background_api_call: ")
        logging.debug(data_dict)
        response = requests.post(url,data=json.dumps(data_dict) ,headers={'Authorization': context['apikey'], 'content-type': 'application/json'})
        logging.debug("background_api_call: "+"request done...")

        
    if response.status_code == 200:
        to = {}
        to["result"] = 'task complete...'
        to['response'] = json.loads(response.text)
        if funct == "resource_create" or funct == "resource_update":
            try:
                to["response"]["result"]["url"] = "/".join(to["response"]["result"]["url"].split("/")[:-1])+"/"+data_dict["file_name"]
            except KeyError:
                pass
    else:
        to = {}
        to["result"] = 'task failed...'
        to['response'] = json.loads(response.text)

    dd = {'to':json.dumps(to), 'id':data_dict['task_id']}
    logging.debug("background_api_call: "+"changing db row...")
    requests.post(
        api_url + '/change_db_row',
        data=json.dumps(dd),
        headers={'Authorization': context['apikey'],
                 'Content-Type': 'application/json'}
    )
    logging.debug("background_api_call: "+"changing db done...")
    if funct == "resource_create" or funct == "resource_update":
        logging.debug("background_api_call: "+"im here...")
        requests.post(
            api_url + '/resource_update',
            data=json.dumps({"id":to["response"]["result"]["id"], "url":data_dict["file_name"]}),
            headers={'Authorization': context['apikey'],
                     'content-type': 'application/json'})
    
    import os
    if fil != None:
        try:
            os.remove(fil)
            dr = fil.split("/")
            dr = dr[:-1]
            os.rmdir("/".join(dr))
        except OSError:
            pass
    return "done..."
