import dash
from qpython import qconnection
from dash.dependencies import Input, Output
import dash_core_components as dcc
import dash_html_components as html
from pandas_datareader import data as web
from datetime import datetime as dt
import datetime
import sys
from spark.Transform import *

bucket_name = 's3a://insighttmpbucket1/'
index_name = bucket_name + 'index.txt'

stock_list = get_stock_list(index_name)

app = dash.Dash()
app.config['suppress_callback_exceptions'] = True

app.layout = html.Div(children=[
    html.Div(children='''
        Input ticker:
    '''),
    dcc.Input(id='input', value='', type='text'),
	html.Div(id='returns-graph'),
	html.Div(id='ret-graph')
]) #, style={'width': '80%', 'display': 'inline-block', 'vertical-align': 'middle'})

#Callback for stock return chart
@app.callback(
    Output(component_id='returns-graph', component_property='children'),
    [Input(component_id='input', component_property='value')]
)
def update_value(input_data):
	stock = input_data.upper()
	stock_key = stock + ".csv"
	if (stock_key not in stock_list):
		print("Sorry, stock ", stock,  " does not exist in our database")
		return None
	query = "select time, close from " + stock
	df = q.sync(query)
	return dcc.Graph(
	id='example-graph',
   	figure={'data': [{'x': df['time'], 'y': df['close'], 'type': 'line', 'name': stock},],
	#'layout': {'height': 400,'margin': {'l': 10, 'b': 20, 't': 0, 'r': 0}}
	})

#Callback for stock return chart
@app.callback(
    Output(component_id='ret-graph', component_property='children'),
    [Input(component_id='input', component_property='value')]
)
def update_value(input_data):
    stock = input_data.upper()
    stock_key = "a_" + stock + ".csv"
    if (stock_key not in stock_list):
        print("Sorry, stock ", stock,  " does not exist in our returns database")
        return None
    query = "select time, close from " + stock
    df = q.sync(query)
    return dcc.Graph(
    id='ret-graph',
    figure={'data': [{'x': df['time'], 'y': df['close'], 'type': 'line', 'name': stock},],
    'layout': {'height': 400,'margin': {'l': 10, 'b': 20, 't': 0, 'r': 0}}
    })



if __name__ == '__main__':
	try:
		q = qconnection.QConnection(host = '10.0.0.10', port = 5100, numpy_temporals = True,pandas = True)
		q.open()
		print("Successful connection")
		
	except:
		print("check kdb port")
	finally:
		app.run_server(debug=True, host='ec2-52-204-67-201.compute-1.amazonaws.com', port=80)
		q.close()

