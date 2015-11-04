__author__ = 'Hao Lin'

from db_drift import db_session, Logs


def log(videoid, detail):
    if db_session.query(Logs).filter_by(videoid=videoid).count():
        log = db_session.query(Logs).filter_by(videoid=videoid).first()
        log.detail = detail
    else:
        db_session.add(Logs(videoid, detail))
