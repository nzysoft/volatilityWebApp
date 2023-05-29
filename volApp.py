"""
Volatility web app + images - companieslogo API
@author: AdamGetbags
"""

# import modules
from flask import Flask, render_template
import sqlite3
# create flask app instance
volApp = Flask(__name__)

def dbConnection():
    conn = sqlite3.connect('volApp.db')
    conn.row_factory = sqlite3.Row
    return conn

# main url
@volApp.route('/')
def createMain():
    conn = dbConnection()
    volatilityData = conn.execute('SELECT * FROM volTable').fetchall()
    return render_template('index.html', volatilityData=volatilityData)