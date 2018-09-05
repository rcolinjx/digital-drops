import collections
import game
import movie


def main():
	movies = movie.GetUpcoming()
	games = game.GetUpcoming()
	
	media = list(set(movies)) + list(set(games))
	media.sort(key=lambda x: x.release_date)
	
	dateDefDict = collections.defaultdict(list)
	
	for medium in media:
		dateDefDict[medium.release_date].append(medium)
	
	for key in dateDefDict.keys():
		for value in dateDefDict[key]:
			print key.strftime('%Y %m %d') + ' - ' + value.title
	
if __name__ == "__main__":
	main()