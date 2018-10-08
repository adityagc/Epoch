import dash
import dash_core_components as dcc
import dash_html_components as html

app = dash.Dash()

app.layout = html.Div('Correlations')

if __name__ == '__main__':
	app.run_server(debug=True, host='ec2-52-204-67-201.compute-1.amazonaws.com', port=80)

