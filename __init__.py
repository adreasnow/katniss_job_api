from db_handler import JobsDB
from flask import Flask
import pandas as pd

def main() -> None:

    app = Flask(__name__)
    JobsDB(generate=True)

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

        # .style
        #                                         .applymap(lambda x: 'color : red'     if x == JobsDB._Status.failed     else '')
        #                                         .applymap(lambda x: 'color : teal'    if x == JobsDB._Status.finished   else '')
        #                                         .applymap(lambda x: 'color : orange'  if x == JobsDB._Status.running    else '')
        #                                         .applymap(lambda x: 'color : blue'    if x == JobsDB._Status.submitted  else '')
        #                                         .applymap(lambda x: 'color : green'   if x == JobsDB._Cluster.m3        else '')
        #                                         .applymap(lambda x: 'color : blue'    if x == JobsDB._Cluster.monarch   else '')

    app.run(host='0.0.0.0', port=5001, threaded=True)


if __name__ == '__main__':
    main()
