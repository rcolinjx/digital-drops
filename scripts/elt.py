from digital_drops.dao import SnowflakeDao
from digital_drops.media.apple import Apple
from digital_drops.media.igdb import Igdb
from digital_drops.media.tmdb import Tmdb


def main():
    dao = SnowflakeDao()

    tmdb = Tmdb(dao)
    tmdb.extract_load()
    tmdb.transform()

    igdb = Igdb(dao)
    igdb.extract_load()
    igdb.transform()

    apple = Apple(dao)
    apple.extract_load()
    apple.transform()


if __name__ == "__main__":
    main()
