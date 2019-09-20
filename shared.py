import json


def get_connection_keys(media_key):
    with open('./config.json') as file_json_config:
        config = json.load(file_json_config)

    return config[media_key]