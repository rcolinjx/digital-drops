from medium import film, game, song
import warehouse


def main():
    conn = warehouse.SnowflakeConnection('database_connection')

    # FILM - TMDB
    tmdb = film.TMDB(conn)
    tmdb.extract_load_upcoming()
    tmdb.transform_upcoming()

    # GAME - IGDB
    igdb = game.IGDB(conn)
    igdb.extract_load_upcoming()
    igdb.transform_upcoming()

    # SONG - Spotify
    spot = song.Spotify(conn)
    spot.extract_load_upcoming()
    spot.transform_upcoming()


if __name__ == "__main__":
    main()
