import uuid
from ckan.lib.celery_app import celery
import logging
import ckan.model as model
from ckan.common import _, c
import json
from celery.result import AsyncResult
import ckan.plugins as p
import ckan.logic
import ckan.logic as logic
import db
import ckan.lib.base as base
from ckan.lib.base import config

def create_background_p_table(context):
    if db.background_api_calls is None:
        db.init_db(context['model'])


def call_function(context, data_dict):
    logging.error(data_dict)
    try:
        logic.check_access(data_dict['function'], context, data_dict)
    except logic.NotAuthorized:
        raise logic.NotAuthorized()
    except ValueError:
        pass
    logging.error("call celery task...")
    
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
    "result":json.dumps({'result':"task started"})
    }

    new_db_row(context,dd)
    data_dict['task_id']  = task_id
    celery.send_task("background_api_call.__call_function", args=[context2, data_dict])

    return {"progress":"task in queue", "task_id":task_id}

@ckan.logic.side_effect_free
def get_result(context, data_dict):
    create_background_p_table(context)

    db.BackgroundApiCalls.delete_old_data(**data_dict)
    session = context['session']
    session.commit()


    logging.error("------------------------------------------------------")
    logging.error(data_dict)
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

