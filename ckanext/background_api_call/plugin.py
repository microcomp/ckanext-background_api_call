import ckan.plugins as plugins
import ckan.plugins.toolkit as toolkit
import ckan.logic as logic
import ckan.logic
import ckan.model as model
from ckan.common import _, c
import api_calls


class BackgroundApiCall(plugins.SingletonPlugin, plugins.toolkit.DefaultDatasetForm):
	plugins.implements(plugins.interfaces.IActions)
	def get_actions(self):
		return {"async_api":api_calls.call_function,
				"action_status_show":api_calls.get_result,
				"del_db_row":api_calls.del_db_row,
				"change_db_row":api_calls.change_db_row}
	