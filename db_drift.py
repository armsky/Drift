from __future__ import absolute_import

from sqlalchemy import *
from sqlalchemy.orm import sessionmaker, scoped_session, relationship, backref
from sqlalchemy.ext.declarative import declarative_base
from settings import DATABASE_OPTION

__author__ = 'Hao Lin'

DATABASE = {
    "DBA_LIVE": {
        "USER": "playplex0mtvglbw",
        "PASS": "rtGTN8zWy",
        "HOST": "live-mysql-01.811.mtvi.com",
        "DBNAME": "drift"
    },
    "DBA_QA":   {
        "USER": "playplex0mtvglbw",
        "PASS": "rWbyltGW",
        "HOST": "qa-mysql-01.1515.mtvi.com",
        "DBNAME": "drift"
    },
    "DBA_DEV":  {
        "USER": "playplex0mtvglbw",
        "PASS": "rWbyltGW",
        "HOST": "dev-mysql-01.1515.mtvi.com",
        "DBNAME": "drift"
    },
    "LOCALHOST": {
        "USER": "",
        "PASS": "",
        "HOST": "localhost",
        "DBNAME": "Drift"
    },
    "204": {
        "USER": "pyuser",
        "PASS": "unicornbacon",
        "HOST": "166.77.22.204",
        "DBNAME": "Drift"
    },
}

database_setting = DATABASE[DATABASE_OPTION]

engine = create_engine('mysql://'+database_setting['USER'] + ':' + database_setting['PASS'] +
                       '@' + database_setting['HOST'] + '/' + database_setting['DBNAME'],
                       convert_unicode=True, pool_recycle=3600, pool_size=10, echo=False)

db_session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=engine))

Base = declarative_base()


class Videos(Base):
    __tablename__ = 'videos'
    __table_args__ = {
        'mysql_engine': 'InnoDB',
        'mysql_charset': 'utf8'
    }

    id = Column(Integer, primary_key=True)
    stateid = Column(Integer, ForeignKey('states.id'), nullable=False)
    state = relationship('States', backref="videos")
    status = Column(String(16), nullable=True)
    title = Column(String(256), nullable=False)
    namespace = Column(String(64), nullable=False)
    showvideouuid = Column(String(36), nullable=False, primary_key=True)
    videoplaylistuuid = Column(String(36), nullable=False)
    episodeuuid = Column(String(36), nullable=False)     # Episode id could be empty, but can't be None
    seriesuuid = Column(String(36), nullable=False)
    uri_1200 = Column(String(256), nullable=False)
    uri_400 = Column(String(256), nullable=False)
    lang = Column(String(8), nullable=False)
    hostentry = Column(String(16), nullable=True)

    def __init__(self, stateid, title, namespace, showvideouuid, videoplaylistuuid, episodeuuid, seriesuuid,
                 uri_1200, uri_400, lang, hostentry=None):
        self.stateid = stateid
        self.title = title
        self.namespace = namespace
        self.showvideouuid = showvideouuid
        self.videoplaylistuuid = videoplaylistuuid
        self.episodeuuid = episodeuuid
        self.seriesuuid = seriesuuid
        self.uri_1200 = uri_1200
        self.uri_400 = uri_400
        self.lang = lang
        self.hostentry = hostentry


class Videoassets(Base):
    __tablename__ = 'videoassets'
    __table_args__ = {
        'mysql_engine': 'InnoDB',
        'mysql_charset': 'utf8'
    }

    id = Column(Integer, primary_key=True)
    videoid = Column(Integer, ForeignKey('videos.id', onupdate='cascade', ondelete='cascade'))
    uri = Column(String(256), nullable=False)
    width = Column(Integer, nullable=False)
    height = Column(Integer, nullable=False)
    bitrate = Column(Integer, nullable=False)
    aspectratio = Column(String(8), nullable=False)
    duration = Column(Integer, nullable=False)
    lang = Column(String(8), nullable=False)

    def __init__(self, videoid, uri, width, height, bitrate, aspectratio, lang, duration=10):
        self.videoid = videoid
        self.uri = uri
        self.width = width
        self.height = height
        self.bitrate = bitrate
        self.aspectratio = aspectratio
        self.lang = lang
        self.duration = duration


class Imageassets(Base):
    __tablename__ = 'imageassets'
    __table_args__ = {
        'mysql_engine': 'InnoDB',
        'mysql_charset': 'utf8'
    }

    id = Column(Integer, primary_key=True)
    videoid = Column(Integer, ForeignKey('videos.id', onupdate='cascade', ondelete='cascade'))
    uri = Column(String(256), nullable=False)
    width = Column(Integer, nullable=False)
    height = Column(Integer, nullable=False)
    aspectratio = Column(String(8), nullable=False)

    def __init__(self, videoid, uri, width, height, aspectratio):
        self.videoid = videoid
        self.uri = uri
        self.width = width
        self.height = height
        self.aspectratio = aspectratio


class States(Base):
    __tablename__ = 'states'
    __table_args__ = {
        'mysql_engine': 'InnoDB',
        'mysql_charset': 'utf8'
    }

    id = Column(Integer, primary_key=True)
    name = Column(String(64), nullable=False)

    def __init__(self, id, name):
        self.id = id
        self.name = name


class Logs(Base):
    __tablename__ = 'logs'
    __table_args__ = {
        'mysql_engine': 'InnoDB',
        'mysql_charset': 'utf8'
    }
    id = Column(Integer, primary_key=True)
    videoid = Column(Integer, ForeignKey('videos.id', onupdate='cascade', ondelete='cascade'))
    video = relationship('Videos', backref="logs")
    detail = Column(String(1024), nullable=False)

    def __init__(self, videoid, detail):
        self.videoid = videoid
        self.detail = detail


class Reports(Base):
    __tablename__ = 'reports'
    __table_args__ = {
        'mysql_engine': 'InnoDB',
        'mysql_charset': 'utf8'
    }
    id = Column(Integer, primary_key=True)
    videoid = Column(Integer, ForeignKey('videos.id', onupdate='cascade', ondelete='cascade'))
    video = relationship('Videos', backref="reports")
    posteruuid = Column(String(36), nullable=False, primary_key=True)
    status = Column(String(16), nullable=False)
    error_spec = Column(String(512), nullable=True)

    def __init__(self, videoid, showvideouuid, status, error_spec):
        self.videoid = videoid
        self.showvideouuid = showvideouuid
        self.status = status
        self.error_spec = error_spec


Base.metadata.create_all(engine, checkfirst=True)

STATE_DICT = {
        1:  "Video Logged",
        2:  "Video Ready To Download",
        3:  "Video Downloading in Queue",
        4:  "Video Download Failed",
        5:  "Video Downloaded",
        6:  "Ffmpeg Generating in Queue",
        7:  "Ffmpeg Failed",
        8:  "Video Processed Successfully",
        9:  "Clip Uploading in Queue",
        10:  "Clip Upload Failed",
        11:  "Clip Uploaded",
        12:  "Xml Generating in Queue",
        13:  "Xml Generate Failed",
        14:  "Xml Generated",
        15:  "Xml Ready To Send/Pickup",
        16:  "Xml Sent To ARC",
        17:  "ARC ID Error",
        18:  "ARC ID Retrieved",
        19:  "Cleanup Failed",
        20:  "Cleaned Up",
        100:  "RESERVED",
        200:  "RESERVED",
        300:  "RESERVED",
    }

# Initialize the states
for stateid, statename in STATE_DICT.iteritems():
    if db_session.query(States).filter_by(id=stateid).count():
        state = db_session.query(States).filter_by(id=stateid).first()
        state.name = statename
    else:
        db_session.add(States(stateid, statename))
db_session.commit()

STATES = {}
states = db_session.query(States).all()
for state in states:
    STATES[state.name] = state.id
