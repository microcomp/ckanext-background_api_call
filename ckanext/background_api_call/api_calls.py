import uuid
from ckan.lib.celery_app import celery
import logging
import ckan.model as model
from ckan.common import _, c
import json

import ckan.plugins as p
import ckan.logic
import ckan.logic as logic
import db
import ckan.lib.base as base
import base64

import cgi
import cgitb; cgitb.enable()
import os, sys

import ckan.lib.munge as munge
from ckan.lib.base import config

class TempUpload(object):
    def __init__(self, resource, folder):
        self.path = folder
        if not self.path:
            self.storage_path = None
            return
        self.storage_path = self.path
        try:
            os.makedirs(self.storage_path)
        except OSError, e:
            ## errno 17 is file already exists
            if e.errno != 17:
                raise
        self.filename = None

        url = resource.get('url')
        upload_field_storage = resource.pop('upload', None)
        self.clear = resource.pop('clear_upload', None)

        if isinstance(upload_field_storage, cgi.FieldStorage):
            self.filename = upload_field_storage.filename
            self.filename = munge.munge_filename(self.filename)
            self.upload_file = upload_field_storage.file
        elif self.clear:
            resource['url_type'] = ''

    def upload(self, filename, max_size=1024):
        filepath = self.path+ filename
        if self.filename:
            try:
                os.makedirs(self.path)
            except OSError, e:
                ## errno 17 is file already exists
                if e.errno != 17:
                    raise
            tmp_filepath = self.path+ filename + '~'
            output_file = open(tmp_filepath, 'wb+')
            self.upload_file.seek(0)
            current_size = 0
            while True:
                current_size = current_size + 1
                #MB chunks
                data = self.upload_file.read(2 ** 20)
                if not data:
                    break
                output_file.write(data)
                if current_size > max_size:
                    os.remove(tmp_filepath)
                    raise logic.ValidationError(
                        {'upload': [_('File upload too large')]}
                    )
            output_file.close()
            os.rename(tmp_filepath, filepath)

        if self.clear:
            try:
                os.remove(filepath)
            except OSError, e:
                pass


def save_uploaded_file (form, upload_dir):
    """This saves a file uploaded by an HTML form.
    The form_field is the name of the file input field from the form.
       For example, the following form_field would be "file_1":
           <input name="file_1" type="file">
       The upload_dir is the directory where the file will be written.
       If no file was uploaded or if the field does not exist then
       this does nothing.
    """
    
    fileitem = form["upload"]
    if not fileitem.file: return
    fout = file (os.path.join(upload_dir, fileitem.filename), 'wb')
    while 1:
        chunk = fileitem.file.readline()
        if not chunk: break
        fout.write (chunk)
    fout.close()


def create_background_p_table(context):
    if db.background_api_calls is None:
        db.init_db(context['model'])


def call_function(context, data_dict):
    try:
        data_dict.get("function")
    except Exception, e:
       raise logic.BadRequest()

    try:
        logic.check_access(data_dict['function'], context, data_dict)
    except logic.NotAuthorized:
        raise logic.NotAuthorized()
    except ValueError:
        pass
    
    user = p.toolkit.get_action('get_site_user')(
        {'model': model, 'ignore_auth': True, 'defer_commit': True}, {}
    )
    context2 = json.dumps({
        'site_url': config.get("ckan.internal_site_url","http://127.0.0.1"),
        'apikey': user.get('apikey')
    })
    try:
        data_dict['function']
    except KeyError, e:
        ed = {'message': 'Function name not set'}
        raise logic.ValidationError(ed)
    if data_dict['function'] == "async_api":
        ed = {'message': 'Function not supported'}
        raise logic.ValidationError(ed)
    task_id = unicode(uuid.uuid4())
    dd = {
    "apikey":user.get('apikey'),
    "task_id":task_id,
    "result":json.dumps({'result':"task sent to celery"})
    }

    new_db_row(context,dd)
    data_dict['task_id']  = task_id
    

    
    if "upload" in data_dict.keys():
        folder_name = "/var/lib/ckan/resources/upload_temp/"+unicode(uuid.uuid4())+"/"
        fn = ""
        fn = data_dict["upload"].filename
        uploader = TempUpload(data_dict,folder_name)
        uploader.upload(fn)
        data_dict["file"] = folder_name+fn
        data_dict["file_name"] = fn
        if "url" not in data_dict.keys():
            data_dict["url"] = "anything"
        if "type" not in data_dict.keys():
            data_dict["type"] = "file.upload"

    celery.send_task("background_api_call.__call_function", args=[context2, data_dict])

    return {"progress":"task in queue", "task_id":task_id}

@ckan.logic.side_effect_free
def get_result(context, data_dict):
    create_background_p_table(context)

    db.BackgroundApiCalls.delete_old_data(**data_dict)
    session = context['session']
    session.commit()

    try:
        info = db.BackgroundApiCalls.get(**data_dict)[0]
        if info.result == 'done':
            del_db_row(**data_dict)
        result = {}
        result['current_task'] = json.loads(info.result)
        
        return result
    except IndexError:
        ed = {'message': 'Wrong task ID'}
        raise logic.ValidationError(ed)

@ckan.logic.side_effect_free
def new_db_row(context, data_dict):
    create_background_p_table(context)
    info = db.BackgroundApiCalls()

    info.apikey = data_dict['apikey']
    info.task_id = data_dict['task_id']
    info.result = data_dict['result']
    info.save()
    session = context['session']
    session.add(info)
    session.commit()
    return {"status":"success"} 


@ckan.logic.side_effect_free
def del_db_row(context, data_dict):
    #id, apikey
    id = data_dict["id"]
    apikey = data_dict["apikey"]
    create_background_p_table(context)
    data_dict = {'task_id':id, 'apikey':apikey}
    db.BackgroundApiCalls.delete(**data_dict)
    return {"status":"success"} 

@ckan.logic.side_effect_free
def change_db_row(context, data_dict):
    #id, to
    id = data_dict["id"]
    to = data_dict["to"]
    create_background_p_table(context)
    info = db.BackgroundApiCalls.get(**{'task_id':id})[0]
    info.result = to
    info.save()
    session = context['session']
    session.commit()
    return {"status":"success"} 
