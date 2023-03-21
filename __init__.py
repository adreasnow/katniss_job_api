from db_handler import JobsDB
from flask import Flask
import pandas as pd
import urllib.request

app = Flask(__name__)
# JobsDB(generate=True)

@app.route('/id/<string:job_ID>/<string:status>/<string:cluster>/', methods=['GET', 'POST'])
def id(job_ID: str, status: str, cluster: str) -> str:
    with JobsDB() as jobsDB:
        out = jobsDB.add_job(job_ID, status, cluster)
    return out

@app.route('/db/', methods=['GET', 'POST'])
def db() -> str:
    with JobsDB() as jobsDB:
        out = jobsDB.query()
    return out

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5151)
