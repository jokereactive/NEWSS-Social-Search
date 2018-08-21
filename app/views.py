from app import app
from flask import render_template
from flask import request
from stats import stats

s=stats()

@app.route('/')
@app.route('/index')

# Landing
def index():
    global s
    answer=s.get_stats()
    return render_template('index.html',ans=answer,debug="")

# Query
@app.route('/', methods=['POST'])
def my_form_post():
    global r2d2
    text = request.form['text']
    query = request.form['check']
    print query
    r2d2=MyCode.r2d2();
    books=r2d2.search(text,query)
    print books
    return render_template('index.html',title='Assignment 1&2: Information Retrieval',answer=books,debug=query)
