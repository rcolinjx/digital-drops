import common
import requests
import datetime
import time


# class Game(object):
# 	id = ''
# 	title = ''
# 	release_date = ''
#
# 	def __init__(self, id, title, release_date):
# 		self.id = id
# 		self.title = title
# 		self.release_date = release_date


class IGDB:
	api_key = 'key_api_game_igdb'

	def extract_load_upcoming(self):
		ms = str(int(round(time.time() * 1000)))

		path = '/release_dates/'
		params = ['filter[date][gt]=' + ms, 'expand=game']
		url = 'https://api-endpoint.igdb.com' + path + '?' + '&'.join(params)

		game_key = common.get_keys()
		headers = {'user-key': game_key, 'Accept': 'application/json'}

		req = requests.get(url, headers=headers)

		res_dict = req.json()

		return res_dict

		# game_list = []
		#
		# for gameDict in res_dict:
		# 	id = gameDict['id']
		# 	title = gameDict['game']['name']
		# 	release_date = datetime.datetime.fromtimestamp((gameDict['game']['first_release_date'])/1000.0)
		#
		# 	game_obj = Game(id, title, release_date)
		#
		# 	game_list.append(game_obj)
		#
		# game_list = list(set(game_list))
		#
		# return game_list
