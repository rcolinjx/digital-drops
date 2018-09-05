import common
import requests
import datetime
import time

class Game(object):
	id = ''
	title = ''
	release_date = ''

	def __init__(self,id,title,release_date):
		self.id = id
		self.title = title
		self.release_date = release_date

def GetUpcoming():
	path = '/release_dates/'

	ms = str(int(round(time.time() * 1000)))
	params = ['filter[date][gt]=' + ms,'expand=game']
	url = FormRequestUrl(path, params)

	gameKey = common.GetKeys('game')
	headers = { 'user-key': gameKey, 'Accept': 'application/json'}

	req = requests.get(url, headers=headers)
	
	resDict = req.json()

	gameList = []

	for gameDict in resDict:
		id = gameDict['id']
		title = gameDict['game']['name']
		releaseDate = datetime.datetime.fromtimestamp((gameDict['game']['first_release_date'])/1000.0)

		gameObj = Game(id,title,releaseDate)

		gameList.append(gameObj)

	return gameList

def FormRequestUrl(path, params):
	url = 'https://api-endpoint.igdb.com' + path + '?' + '&'.join(params)

	return url