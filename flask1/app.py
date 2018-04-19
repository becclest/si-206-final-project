from flask import Flask, render_template, request
import plotly.plotly as py
from plotly.graph_objs import *

app = Flask(__name__)


@app.route("/")
def index():
    return render_template('index.html')


@app.route('/graphs')
def return_graphs():
    return render_template('graphs.html')


if __name__ == '__main__':
    app.run(debug=True)
