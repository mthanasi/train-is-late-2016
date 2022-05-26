from datetime import datetime
import tqdm
import glob
import sys

import typing
import pymysql
import pymysql.cursors

import utils


def s2dt(s: str) -> typing.Optional[datetime]:
    """Parses a string and turns it into a datetime object.

    :param s: String to parse.
    """

    if s == "":
        return None

    return datetime.strptime(s, "%d.%m.%Y %H:%M")


def date2date(s: str) -> typing.Optional[datetime]:
    """Parses a string and turns it into a date object."""

    if s == "":
        return None

    return datetime.strptime(s, "%d.%m.%Y")


def parse_line(line: str) -> dict:
    """Parses a single line of the SBB CSV file.

    :param line: Line of the CSV file to parse.
    """
    ll = line[:-1].split(";")
    d = {
        "day": date2date(ll[0]),
        "ride_desc": ll[1],
        "comp_id": ll[2],
        "c_sname": ll[3],
        "c_name": ll[4],
        "prod_id": ll[5],
        "line_id": int(ll[6]) if ll[6] is not "" else None,
        "line_txt": ll[7],
        "c_id": ll[8],
        "v_txt": ll[9],
        "add_ride": ll[10] == "true",
        "trip_canc": ll[11] == "true",
        "stop_id": int(ll[12]) if ll[12] is not "" else None,
        "stop_name": ll[13],
        "arr_time": s2dt(ll[14]),
        "arr_est": s2dt(ll[15]),
        "est_stat": ll[16],
        "dep_time": s2dt(ll[17]),
        "dep_est": s2dt(ll[18]),
        "dep_estat": ll[19],
        "train_skip": ll[20] == "true",
    }
    return d


def fmt_option(dt: typing.Optional[datetime], fmt: str) -> typing.Optional[str]:
    """Formats a datetime object according to a format string, provided it
    is not NULL.

    :param dt: Datetime object to format.
    """

    if dt is None:
        return "NULL"
    else:
        return dt.strftime(fmt)


def fmt_sql(l: dict) -> str:
    """Formats a single data item into an executable SQL string.

    :param l: Data line to parse.
    """

    return (
        'INSERT INTO SbbData VALUES ("{}","{}","{}","{}","{}","{}",'
        '{},"{}","{}","{}",{},{},{},"{}","{}","{}","{}","{}","{}","{}",'
        "{}); ".format(
            fmt_option(l["day"], "%Y-%M-%D"),
            fmt_option(l["ride_desc"]),
            fmt_option(l["comp_id"]),
            fmt_option(l["c_sname"]),
            fmt_option(l["c_name"]),
            fmt_option(l["prod_id"]),
            fmt_option(l["line_id"]),
            fmt_option(l["line_txt"]),
            fmt_option(l["c_id"]),
            fmt_option(l["v_txt"]),
            str(l["add_ride"]).upper(),
            str(l["trip_canc"]).upper(),
            fmt_option(l["stop_id"]),
            fmt_option(l["stop_name"]),
            fmt_option(l["arr_time"], "%Y-%m-%d %H:%M:00"),
            fmt_option(l["arr_est"], "%Y-%m-%d %H:%M:00"),
            fmt_option(l["est_stat"]),
            fmt_option(l["dep_time"], "%Y-%m-%d %H:%M:00"),
            fmt_option(l["dep_est"], "%Y-%m-%d %H:%M:00"),
            fmt_option(l["dep_estat"]),
            str(l["train_skip"]).upper(),
        )
    )


def read_and_store(infile: str, host: str, username: str, database: str, password: str):
    """Reads a data file in the SBB data format and exports it into an SQL
    database.


    :param infile: Path to input file.
    :param host: Hostname of SQL server.
    :param username: Username to use for SQL server.
    :param database: Database to connect to.
    :param password: Password to access database.
    """

    # connect to the db
    db = pymysql.connect(
        host=host, user=username, password=password, db=database, cursorclass=pymysql.cursors.DictCursor
    )

    # read the file
    with open(infile, "r") as f:
        lines = f.readlines()

    # remove the first line and parse
    lines.pop(0)
    sqllines = [fmt_sql(parse_line(line)) for line in lines]

    # get ready to run something
    cursor = db.cursor()

    # create table if it doesn't already exist
    cursor.execute(
        "CREATE TABLE IF NOT EXISTS `SbbData` (`day` date DEFAULT"
        " NULL,`ride_desc` varchar(255) DEFAULT NULL,`comp_id` "
        "varchar(255) DEFAULT NULL,`c_sname` varchar(255) DEFAULT "
        "NULL,`c_name` varchar(255) DEFAULT NULL,`prod_id` "
        "varchar(255) DEFAULT NULL,`line_id` int(11) DEFAULT NULL,"
        "`line_txt` varchar(255) DEFAULT NULL,`c_id` varchar(255) "
        "DEFAULT NULL,`v_txt` varchar(255) DEFAULT NULL,`add_ride` "
        "tinyint(1) DEFAULT NULL,`trip_canc` tinyint(1) DEFAULT "
        "NULL,`stop_id` int(11) DEFAULT NULL,`stop_name` "
        "varchar(255) DEFAULT NULL,`arr_time` datetime DEFAULT NULL,"
        "`arr_est` datetime DEFAULT NULL,`est_stat` varchar(255) "
        "DEFAULT NULL,`dep_time` datetime DEFAULT NULL,`dep_est` "
        "datetime DEFAULT NULL,`dep_estat` varchar(255) DEFAULT "
        "NULL,`train_skip` tinyint(1) DEFAULT NULL);"
    )

    # start a transaction
    cursor.execute("START TRANSACTION;")

    # define CHUNK_SIZE and current index
    CHUNK_SIZE = 5000
    idx = CHUNK_SIZE

    # insert chunks of CHUNK_SIZE
    while idx < len(sqllines):
        cursor.execute("".join(sqllines[idx - CHUNK_SIZE : idx]))
        idx += CHUNK_SIZE

    # finally commit everything and close the connection to the DB.
    cursor.execute("COMMIT;")

    # and make a nice index -- this will take forever
    cursor.execute("CREATE INDEX PIndex IF NOT EXISTS ON SbbData (stop_id);")
    db.close()


def main():
    """Main entry point for import script."""

    # check that we have enough arguments
    if "--help" in sys.argv[1:]:
        print(
            """Reads in SBB CSV files and imports them into an SQL database.
        Usage: {} [PATH [HOST [USER [DATABASE [PASSWORD]]]]]
        """.format(
                sys.argv[0]
            )
        )
        sys.exit(1)

    # read all of the arguments
    path = utils.read_or_ask(1, "Path to the files to import>")
    host = utils.read_or_ask(2, "SQL Hostname>")
    username = utils.read_or_ask(3, "Username for {}>".format(host))
    database = utils.read_or_ask(4, "Database Name>")
    password = utils.read_or_ask(5, "Password for {} ({}@{})>".format(database, username, host), True)

    # read all files in the given directory
    for f in tqdm.tqdm(glob.glob(path + "/*.csv")):
        read_and_store(f, host, username, database, password)


if __name__ == "__main__":
    main()
