import common
import datetime
import json
import requests


# class Movie(object):
# 	id = ''
# 	title = ''
# 	release_date = ''
#
# 	def __init__(self, id, title, release_date):
# 		self.id = id
# 		self.title = title
# 		self.release_date = release_date


class TMDB:
	api_key = 'key_api_film_tmdb'
	table = 'DEV_STAGING_TRANSIENT.API.FILM_TMDB'

	def extract_load_upcoming(self, cur):
		path = '/movie/upcoming'
		params = ['language=EN', 'region=US']
		url = 'https://api.themoviedb.org/3' + path + '?' + '&'.join(params) + '&api_key=' + self.api_key

		req = requests.get(url)

		res = req.text

		cur.execute('INSERT INTO {0}(JSON) SELECT PARSE_JSON($${1}$$)'.format(self.table, res))

		'''TODO: find the pages'''

		# payload_list = res_dict['results']
		#
		# movie_list = []
		#
		# for movie_dict in payload_list:
		# 	id = movie_dict['id']
		# 	title = movie_dict['title']
		# 	release_date = datetime.datetime.strptime(movie_dict['release_date'], '%Y-%m-%d')
		#
		# 	movie_obj = Movie(id, title, release_date)
		#
		# 	movie_list.append(movie_obj)
		#
		# movie_list = list(set(movie_list))
		#
		# return movie_list
