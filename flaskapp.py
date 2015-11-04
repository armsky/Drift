from flask import Flask, request, jsonify, abort
import os
import json

from db_drift import db_session, Videos, Logs, Reports, STATES

__author__ = 'Hao Lin'


app = Flask(__name__)
PORT = '5000'
REPORT_PATH = '/drift/report'


@app.route(REPORT_PATH, methods=['PUT', 'POST'])
def report_drift():
    report_json = json.loads(request.data)['report']
    workflowtype = report_json["workflowtype"]
    if workflowtype == "posterclip":
        posteruuid = report_json["unique_key"]
        showvideouuid = report_json["showvideouuid"]
        status = report_json["asset_status"]
        error_spec = report_json["error_spec"]

        video = db_session.query(Videos).filter_by(showvideouuid=showvideouuid).first()
        videoid = video.id
        if db_session.query(Reports).filter_by(videoid=videoid).count():
            report = db_session.query(Reports).filter_by(videoid=videoid)
            report.posteruuid = posteruuid
            report.status = status
            report.error = error_spec
        else:
            report = Reports(videoid, posteruuid, status, error_spec)
            db_session.add(report)
        # Update Videos table with status
        if status == "success":
            video.status = STATES['ARC ID Retrieved']
        else:
            video.status = STATES['ARC ID Error']

        db_session.commit()
    return "report success"


if __name__ == '__main__':
    port = os.getenv('VCAP_APP_PORT', PORT)

    app.run(host="0.0.0.0", port=int(port), debug=True)
