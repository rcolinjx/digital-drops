from app_elt.classes.media import album
from app_elt.classes import database


def main():
    conn = database.SnowflakeConnection('database_connection')

    # FILM - TMDB
    # tmdb = film.TMDB(conn)
    # tmdb.extract_load_upcoming()
    # tmdb.transform_upcoming()

    # GAME - IGDB
    # igdb = game.IGDB(conn)
    # igdb.extract_load_upcoming()
    # igdb.transform_upcoming()

    # ALBUM - Apple
    appl = album.Apple(conn)
    # appl.extract_load_upcoming()
    appl.transform_upcoming()


if __name__ == "__main__":
    main()
