import logging
import time

import psycopg2
from psycopg2 import pool
from dota2.client import Dota2Client
from steam.client import SteamClient

import config

logging.basicConfig(
    format="{asctime} | {levelname:<7} | {funcName:<30} | {message}",
    datefmt="%H:%M:%S %d/%m",
    style="{",
    level=logging.INFO,
)


log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

steam = SteamClient()
dota = Dota2Client(steam)
pql_pool = pool.ThreadedConnectionPool(5, 20, config.POSTGRES_URL)
connection = pql_pool.getconn()

top_source = {}


@dota.on("top_source_tv_games")
def print_profile_card(result):
    log.info(
        f"top_source_tv_games resp ng: {result.num_games} sg: {result.specific_games} "
        f"{result.start_game, result.game_list_index, len(result.game_list)} "
        f"{result.game_list[0].players[0].account_id}"
    )

    cursor = connection.cursor()
    cursor.execute("select * from user_settings")
    mobile_records = cursor.fetchall()
    for row in mobile_records:
        print(row)
    cursor.close()

    for match in result.game_list:
        top_source[match.match_id] = match

    if len(top_source) == 100:
        dota.emit("my_top_games_response")


steam.login(username=config.STEAM_USERNAME, password=config.STEAM_PASSWORD)
dota.launch()


while True:
    log.info(f"--- Task is starting now ---")
    start_time = time.perf_counter()
    top_source = {}
    dota.request_top_source_tv_games(start_game=90)
    dota.wait_event("my_top_games_response", timeout=8)
    log.info(f"top source request took {time.perf_counter() - start_time} secs")
    log.info(f"--- Task is finished ---")
    time.sleep(10)

    # implement that arry idea where we keep new and previous top_source dict
    # compare them 
    # add new ones into database
    # remove old ones from the database
    