import dash
from qpython import qconnection
from dash.dependencies import Input, Output
import dash_core_components as dcc
import dash_html_components as html
from pandas_datareader import data as web
from datetime import datetime as dt
import datetime


app = dash.Dash()

app.layout = html.Div(children=[
    html.Div(children='''
        Symbol to graph:
    '''),
    dcc.Input(id='input', value='', type='text'),
	html.Div(id='output-graph'),
])

@app.callback(
    Output(component_id='output-graph', component_property='children'),
    [Input(component_id='input', component_property='value')]

)


def update_value(input_data):
    #start = datetime.datetime(2015, 1, 1)
    #end = datetime.datetime.now()
    #df = web.DataReader(input_data, 'morningstar', start, end)
	#Code to covert input_data to ticker
	try:
		df = q.sync('{select time, string x from table1}', input_data)
		print(df.head())
		return dcc.Graph(
		id='example-graph',
   	 	figure={'data': [{'x': df['time'], 'y': df[input_data], 'type': 'line', 'name': input_data},],'layout': {'title': input_data}})
	except:
		return None


if __name__ == '__main__':
	try:
		q = qconnection.QConnection(host = '10.0.0.10', port = 5001, numpy_temporals = True,pandas = True)
		q.open()
	except:
		print("Please set up KDB+ server at port 5001")
	finally:
		#app.run_server(debug=True, host='ec2-52-204-67-201.compute-1.amazonaws.com', port=80)
		input_data = 'ATVI'
		df = q.sync('{select time, string x from table1}', input_data)
        print(df.head())
		#data = q.sync('select time, ABT from table1')
		#print(type(data))
		q.close()

