import json


def GetKeys(mediaKey):
	with open('./config.json') as file_json_config:
		config = json.load(file_json_config)
	
	return config[mediaKey + '_api_key']