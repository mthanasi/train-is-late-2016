import flask
import sys
import json

import pymysql
import pymysql.cursors

import utils

#
# CONFIG
#

# create an app
app = flask.Flask(__name__)

# read all the default arguments
if "--help" in sys.argv[1:]:
    print(
        """Reads in SBB CSV files and imports them into an SQL database.
       Usage: {} [HOST [USER [DATABASE [PASSWORD]]]]
       """.format(
            sys.argv[0]
        )
    )
    sys.exit(1)

# read all of the arguments
host = utils.read_or_ask(1, "SQL Hostname>")
username = utils.read_or_ask(2, "Username for {}>".format(host))
database = utils.read_or_ask(3, "Database Name>")
password = utils.read_or_ask(4, "Password for {} ({}@{})>".format(database, username, host), True)

# connect to the db
db = pymysql.connect(
    host=host, user=username, password=password, db=database, charset="utf8mb4", cursorclass=pymysql.cursors.DictCursor
)


#
# ROUTES
#


@app.route("/", methods=["GET"])
def index() -> str:
    """Renders the homepage."""

    return flask.render_template("index.html")


@app.route("/style.css", methods=["GET"])
def style() -> str:
    """Renders the stylesheet."""

    return flask.render_template("style.css")


@app.route("/stop.json", methods=["GET"])
def stop() -> str:
    """Renders status information for a stop."""

    # Check that we have an 'id' parameter
    sid = flask.request.args.get("id")
    if sid is None:
        return stop_error("Missing required parameter 'id'. ")

    # Make sure it is an int.
    try:
        sid = str(int(sid))
    except:
        return stop_error("Invalid value for parameter id. ")

    # Make a request for things.
    QUERY_ALL = "SELECT COUNT(*) FROM SbbData WHERE stop_id={}".format(sid)
    QUERY_LATE = "SELECT COUNT(*), AVG(TIME_TO_SEC(dep_time) - TIME_TO_SEC(dep_est)) FROM SbbData WHERE stop_id={} AND ((arr_time != arr_est) OR (dep_time != dep_est) OR trip_canc OR train_skip)".format(
        sid
    )
    QUERY_DELAYS = "SELECT TIME_TO_SEC(dep_time), TIME_TO_SEC(dep_time) - TIME_TO_SEC(dep_est) FROM SbbData WHERE stop_id={} AND ((arr_time != arr_est) OR (dep_time != dep_est) OR trip_canc OR train_skip)".format(
        sid
    )

    with db.cursor() as cursor:
        cursor.execute(QUERY_ALL)
        count_all = cursor.fetchone()
    with db.cursor() as cursor:
        cursor.execute(QUERY_LATE)
        count_late = cursor.fetchone()
    with db.cursor() as cursor:
        cursor.execute(QUERY_DELAYS)
        count_delays = cursor.fetchall()

    count_delays = [["time", "delay"]] + list(
        filter(
            lambda q: q[1] != None and q[1] < 500 and q[1] > 0,
            map(
                lambda r: [r["TIME_TO_SEC(dep_time)"], r["TIME_TO_SEC(dep_time) - TIME_TO_SEC(dep_est)"]], count_delays
            ),
        )
    )

    # render the result.
    return flask.render_template(
        "stop/stop.json",
        id=sid,
        all=count_all["COUNT(*)"],
        late=count_late["COUNT(*)"],
        delay=count_late["AVG(TIME_TO_SEC(dep_time) - TIME_TO_SEC(dep_est))"],
        delays=json.dumps(count_delays),
    )


def stop_error(message: str):
    """Renders an error request for the /stop.json route.

    :param message: Error message to show.
    """

    return flask.render_template("stop/error.json", message=message)


#
# RUNNING
#

if __name__ == "__main__":
    app.run()
