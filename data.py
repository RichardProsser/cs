# Derived from https://wiki.postgresql.org/wiki/Using_psycopg2_with_PostgreSQL
import psycopg2
import psycopg2.extras
import logging
from flask.json import jsonify
import flask


logging.basicConfig(format='%(asctime)s - %(lineno)s - %(levelname)s - %(message)s',
                    filename='cs.log',
                    filemode='w',
                    level=logging.DEBUG)

conn_string = "host='localhost' dbname='postgres' user='postgres' password='postgres'"


def get_db():
    """ Opens a new database connection if necessary """
    if not hasattr(flask.g, 'conn'):
        logging.info("Connecting to database\n	->{}".format(conn_string))

        try:
            flask.g.conn = psycopg2.connect(conn_string)
        except psycopg2.Error as e:
            logging.error(e)
            flask.abort(500, {'message': e})
    return flask.g.conn


# TODO: protect against SQL Injection.
def execute(sql):
    try:
        # conn.cursor will return a cursor object, you can use this query to perform queries.
        # Note that in this example we pass a cursor_factory argument that will be a
        # dictionary cursor so COLUMNS will be returned as a dictionary so we
        # can access columns by their name instead of index.
        conn = get_db()
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cursor.execute(sql)
        row_count = cursor.rowcount
    except psycopg2.Error as e:
        logging.error(e)
        flask.abort(500, {'message': e})

    # Only return rows if there are any!
    result = None if row_count == -1 else cursor.fetchall()
    return jsonify(result)


def query(where_clause):
    """ Execute 'select' statement """

    stmt = "select * from import.cityscience where {};".format(where_clause)
    logging.debug(stmt)
    return execute(stmt)


# URL functions ...
# N.B.: all data is in text (string) format.
def get_count_point(count_point, year):
    where = "CP = '{}' and AADFYear = '{}'".format(count_point, year)
    return query(where)


def get_road(road_name, year):
    where = "Road = '{}' and AADFYear = '{}'".format(road_name, year)
    return query(where)


# Simple test.
def main():

    # tell postgres to use more work memory
    work_mem = 2048
    stmt = 'SET work_mem TO {}'.format(work_mem)
    execute(stmt)
    result = execute('SHOW work_mem')

    # access the column by numeric index:
    # even though we enabled columns by name I'm showing you this to
    # show that you can still access columns by index and iterate over them.
    # print("Value: ", memory[0])

    # Show the entire row (only one, in this case).
    logging.info("Row:	{}".format(result))


if __name__ == "__main__":
    main()
