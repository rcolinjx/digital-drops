import common
import datetime
import requests

class Movie(object):
	id = ''
	title = ''
	release_date = ''

	def __init__(self,id,title,release_date):
		self.id = id
		self.title = title
		self.release_date = release_date

def GetUpcoming():
	path = '/movie/upcoming'
	params = ['language=EN','region=US']
	url = FormRequestUrl(path, params)

	req = requests.get(url)

	resDict = req.json()

	'''TODO: find the pages'''

	payloadList = resDict['results']

	movieList = []

	for movieDict in payloadList:
		id = movieDict['id']
		title = movieDict['title']
		releaseDate = datetime.datetime.strptime(movieDict['release_date'],'%Y-%m-%d')

		movieObj = Movie(id,title,releaseDate)

		movieList.append(movieObj)

	return movieList

def FormRequestUrl(path, params):
	movieKey = common.GetKeys('movie')

	url = 'https://api.themoviedb.org/3' + path + '?' + '&'.join(params) + '&api_key=' + movieKey

	return url