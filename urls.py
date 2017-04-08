from flask import Flask
import data

app = Flask(__name__)


@app.before_request
def init_db_connection():
    data.get_db()


@app.route('/')
def ok():
    return 'OK'


@app.route('/cp/<count_point>/<year>/')
def cp(count_point, year):
    return data.get_count_point(count_point, year)


@app.route('/road/<road_name>/<year>/')
def road(road_name, year):
    return data.get_road(road_name, year)


if __name__ == "__main__":
    app.run(debug=True)
