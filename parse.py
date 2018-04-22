from datetime import datetime
import pymysql.cursors
from tqdm import tqdm


def date2time(x):
    if x == "":
        return None
    return datetime.strptime(x, '%d.%m.%Y %H:%M')

def parse_line(line):
    ll = line[:-1].split(";")
    d = {
        "day": datetime.strptime(ll[0], '%d.%m.%Y'),
        "ride_desc" : ll[1],
        "comp_id" : ll[2],
        "c_sname" : ll[3],
        "c_name" : ll[4],
        "prod_id" : ll[5],
        "line_id": int(ll[6]),
        "line_txt": ll[7],
        "c_id" : ll[8],
        "v_txt" : ll[9],
        "add_ride" : ll[10]  == "true",
        "trip_canc" : ll[11] == "true",
        "stop_id" : int(ll[12]),
        "stop_name" : ll[13],
        "arr_time" : date2time(ll[14]),
        "arr_est" : date2time(ll[15]),
        "est_stat" : ll[16],
        "dep_time" : date2time (ll[17]),
        "dep_est" : date2time(ll[18]),
        "dep_estat" : ll[19],
        "train_skip" : ll[20] == "true",
    }
    return d

def print_dt(dt, fmt):
    if dt is None:
        return "NULL"
    else:
        return dt.strftime(fmt)

def fmt_sql(pl):
    return """INSERT INTO SbbData VALUES (
      "{}",
      "{}",
      "{}",
      "{}",
      "{}",
      "{}",
      {},
      "{}",
      "{}",
      "{}",
      {},
      {},
      {},
      "{}",
      "{}",
      "{}",
      "{}",
      "{}",
      "{}",
      "{}",
      {}
); """.format(
    print_dt(pl["day"], "%Y-%M-%D"),
    pl["ride_desc"],
    pl["comp_id"],
    pl["c_sname"],
    pl["c_name"],
    pl["prod_id"],
    pl["line_id"],
    pl["line_txt"],
    pl["c_id"],
    pl["v_txt"],
    str(pl["add_ride"]).upper(),
    str(pl["trip_canc"]).upper(),
    pl["stop_id"],
    pl["stop_name"],
    print_dt(pl["arr_time"], "%Y-%m-%d %H:%M:00"),
    print_dt(pl["arr_est"], "%Y-%m-%d %H:%M:00"),
    pl["est_stat"],
    print_dt(pl["dep_time"], "%Y-%m-%d %H:%M:00"),
    print_dt(pl["dep_est"], "%Y-%m-%d %H:%M:00"),
    pl["dep_estat"],
    str(pl["train_skip"]).upper()
)

def main(infile, host, username, database, password):

    db = pymysql.connect(host=host,
                             user=username,
                             password=password,
                             db=database,
                             cursorclass=pymysql.cursors.DictCursor)

    with open("data.csv", "r") as f:
        lines = f.readlines()

    lines.pop(0)
    sqllines = [fmt_sql(parse_line(line)) for line in lines]

    # start a transaction
    cursor = db.cursor()
    cursor.execute("START TRANSACTION;")

    # submit 1000 lines at a time
    CHUNK_SIZE = 1000
    idx = CHUNK_SIZE

    while idx < len(sqllines):
        cursor.execute(
            ''.join(sqllines[idx-CHUNK_SIZE:idx])
        )
        idx += CHUNK_SIZE

    # commit everything and close
    cursor.execute("COMMIT;")
    db.close()

if __name__ == "__main__":
    main("data.csv", "hw.is", "sbb", "sbb", "3UXZ0ZGSNOMVINO5")
