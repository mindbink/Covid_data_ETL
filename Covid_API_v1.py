import flask
from flask import request, jsonify
import mysql.connector


config = {'user':'root',
		  'host':'localhost',
		  'password':'password',
		  'database':'github'
}

# connect to database
def mysql_connect(query, to_filter):
	conn = mysql.connector.connect(**config)
	cursor = conn.cursor(dictionary=True)
	results = cursor.execute(query, to_filter)
	results = cursor.fetchall()
	cursor.close()
	return jsonify(results)


app = flask.Flask(__name__)
app.config['DEBUG'] = True


# home page
@app.route('/', methods=['GET'])
def home():
	return '''<h1>Covid-19 data archive</h1>
	<p>This site is a API for retrieving Codiv-19 data.</p>
	<ul>
		<li>To get <b>all data</b> use path:
			/api/v1/resources/data/all</li>
		<li>To get <b>country</b> use path:
			/api/v1/resources/data?country=<b>[country/region_name]</b>
		<li>To get number of <b>total cases</b> use path:
			/api/v1/resources/total_cases?country=<b>[country/region_name]</b></li>
		<li>To get results by <b>date</b> use path:
			/api/v1/resources/data?date=<b>YYYY-MM-DD</b></li>	
			</ul>'''


# all available data 
@app.route('/api/v1/resources/data/all', methods=['GET'])
def app_all():
	conn = mysql.connector.connect(**config)
	cursor = conn.cursor(dictionary=True)
	all_data = cursor.execute('SELECT * FROM archive LIMIT 50000;')
	all_data = cursor.fetchall()
	cursor.close()

	return jsonify(all_data)


# wrong url handler
@app.errorhandler(404)
def page_not_found(e):

	return '<h1>404</h1><p>The resource could not be found.</p>', 404


# get total cases by country 
@app.route('/api/v1/resources/total_cases')
def total_cases():
	query_param = request.args
	country = query_param.get('country')

	query = 'SELECT Country_Region, SUM(Confirmed_cases) total_cases FROM archive WHERE Last_Update=date(now()) - INTERVAL 1 DAY AND'
	to_filter = []

	if country:
		query += ' Country_Region=%s;'
		to_filter.append(country)
	if not (country):
		return page_not_found(404)

	return mysql_connect(query, to_filter)


#  filter covid-19 data by specified parameter
@app.route('/api/v1/resources/data', methods=['GET'])
def api_filter():
	query_param = request.args
	country = query_param.get('country')
	date = query_param.get('date')

	query = 'SELECT * FROM archive WHERE'
	to_filter = []

	if country:
		query += ' Country_Region=%s AND'
		to_filter.append(country)
	if date:
		query += ' Last_Update=%s AND'
		to_filter.append(date)
	if not (country or date):
		return page_not_found(404)

	query = query[:-4] + ';'

	return mysql_connect(query, to_filter)

app.run()