import dash
from qpython import qconnection
from dash.dependencies import Input, Output
import dash_core_components as dcc
import dash_html_components as html
from pandas_datareader import data as web
from datetime import datetime as dt
import datetime


app = dash.Dash()
stock_list = ['ABT', 'AOS', 'ATVI', 'ABBV']

app.layout = html.Div(children=[
    html.Div(children='''
        Symbol to graph:
    '''),
    dcc.Input(id='input', value='', type='text'),
	html.Div(id='returns-graph'),
	#html.Div(id='correlations-graph'),
])

#Callback for stock return chart
@app.callback(
    Output(component_id='returns-graph', component_property='children'),
    [Input(component_id='input', component_property='value')]
)
def update_value(input_data):
	#FIX THIS LATER FOR ALL DATA
	if (not input_data in stock_list):
		print("Sorry, stock ", input_data,  " does not exist in our database")
		return None
	table_name = "returns"
	query = "select time, " + input_data + " from " + table_name
	#print(query)
	df = q.sync(query)
	#print(df.head())
	return dcc.Graph(
	id='example-graph',
   	figure={'data': [{'x': [0.8, -0.5, 0.3], 'y': ["AOS", "ATVI", "ABBV" ] , 'type': 'bar', 'name': input_data},],'layout': {'title': input_data}})
	#except:
	#	print("An error occurred while trying to get data from dataframe")
	#	return None

"""
#Callback for correlations chart
@app.callback(
    Output(component_id='correlations-graph', component_property='children'),
    [Input(component_id='input', component_property='value')]
)
def update_value(input_data):
    #FIX THIS LATER FOR ALL DATA
    if (not input_data in stock_list):
        print("Sorry, stock ", input_data,  " does not exist in our database")
        return None
    table_name = "returns"
    query = "select time, " + input_data + " from " + table_name
    #print(query)
    df = q.sync(query)
    #print(df.head())
    return dcc.Graph(
    id='example-graph',
    figure={'data': [{'x': df['time'], 'y': df[input_data], 'type': 'line', 'name': input_data},],'layout': {'title': input_data}})
    #except:
    #   print("An error occurred while trying to get data from dataframe")
    #   return None
"""

if __name__ == '__main__':
	try:
		q = qconnection.QConnection(host = '10.0.0.10', port = 5001, numpy_temporals = True,pandas = True)
		q.open()
		print("Successful connection")
		
	except:
		print("Please set up KDB+ server at port 5001")
	finally:
		app.run_server(debug=True, host='ec2-52-204-67-201.compute-1.amazonaws.com', port=80)
		q.close()

