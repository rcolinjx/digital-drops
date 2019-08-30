import common
from medium import film, game
import snowflake.connector


def main():

    db_keys = common.get_keys("db_connection")

    conn = snowflake.connector.connect(
        user=db_keys['user'],
        password=db_keys['password'],
        account=db_keys['account'],
        warehouse=db_keys['warehouse'],
        database=db_keys['database'],
        schema=db_keys['schema']
    )

    cur = conn.cursor()

    tmdb = film.TMDB()
    tmdb.extract_load_upcoming(cur)

    # igdb = game.IGDB()
    # igdb.extract_load_upcoming(cur)

# + list(set(games))
#
# media.sort(key=lambda x: x.release_date)
#
# date_def_dict = collections.defaultdict(list)
#
# for medium in media:
# 	date_def_dict[medium.release_date].append(medium)
#
# for key in date_def_dict.keys():
# 	for value in date_def_dict[key]:
# 		print(key.strftime('%Y %m %d') + ' - ' + value.title)
#
# conn = database.connect_to_schema("FILM_TMDB")
#
# database.load_to_table(conn, media)


if __name__ == "__main__":
    main()
