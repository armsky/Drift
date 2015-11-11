import os
import subprocess
import settings
import shutil
from celeryapp import app
from celery.utils.log import get_task_logger
from celery.exceptions import SoftTimeLimitExceeded
from sqlalchemy.exc import SQLAlchemyError
from ftplib import FTP, all_errors
import xml.etree.ElementTree as ET
from lxml import etree

from db_drift import db_session, Videos, Videoassets, Imageassets, Logs, STATES

__author__ = 'Hao Lin'

CREDENTIAL = settings.FTP_CREDENTIAL

logger = get_task_logger(__name__)


class SqlAlchemyTask(app.Task):
    """An abstract Celery Task that ensures that the connection to the
    database is closed on task completion"""
    @staticmethod
    def after_return(status, retval, task_id, args, kwargs, einfo):
        db_session.remove()


@app.task(base=SqlAlchemyTask)
def ftpget(videoid, folder_path):
    try:
        video = db_session.query(Videos).filter_by(id=videoid).first()
        print "video id is: " + str(video.id)
        if video.hostentry != "mtviestor":
            ftp = FTP(CREDENTIAL["HOST"], CREDENTIAL["USER"], CREDENTIAL["PASS"])
            print ftp
        else:
            ENTRY = settings.FTP_ENTRY
            ftp = FTP(ENTRY["mtviestor"]["HOST"], ENTRY["mtviestor"]["USER"], ENTRY["mtviestor"]["PASS"])
        local_path_1200 = os.path.join(folder_path, video.uri_1200.split('/')[-1])
        local_path_400 = os.path.join(folder_path, video.uri_400.split('/')[-1])
        print "Downloading: " + video.uri_1200 + " == >" + local_path_1200
        ftp.retrbinary("RETR " + video.uri_1200, open(local_path_1200, 'wb').write)
        print "Downloading" + video.uri_400 + " == >" + local_path_400
        ftp.retrbinary("RETR " + video.uri_400, open(local_path_400, 'wb').write)

        ftp.quit()
        video.stateid = STATES['Video Downloaded']
        db_session.commit()
    except all_errors, e:
        # TODO: Catch FTP errors
        db_session.add(Logs(videoid, str(e)))
        video.stateid = STATES['Video Download Failed']
        db_session.commit()
        return
    except SQLAlchemyError, e:
        # TODO: Catch SQLAlchemy errors
        return
    except Exception, e:
        # TODO: Catch Other errors
        db_session.add(Logs(videoid, str(e)))
        video.stateid = STATES['Video Download Failed']
        db_session.commit()
    return


@app.task(base=SqlAlchemyTask)
def localcopy(videoid, folder_path):
    try:
        ENTRY = settings.FTP_ENTRY
        video = db_session.query(Videos).filter_by(id=videoid).first()
        print "video id is: " + str(video.id)
        if video.hostentry == "alias":
            mount_point = ENTRY['alias']['LOCAL_MOUNT']
            if not os.path.isdir(mount_point):
                error_message = "mount point not visible: " + mount_point + ". Please check."
                print error_message
                db_session.add(Logs(videoid, error_message))
                db_session.commit()
                return
        elif video.hostentry == "scenic":
            # TODO: Need to implemented later
            print "not support scenic yet."
            return
        else:
            print "not support", video.hostentry, "yet."
            return
        local_path_1200 = os.path.join(folder_path, video.uri_1200.split('/')[-1])
        local_path_400 = os.path.join(folder_path, video.uri_400.split('/')[-1])
        print "copying: " + video.uri_1200 + " == >" + local_path_1200
        shutil.copyfile(video.uri_1200, local_path_1200)
        print "copying: " + video.uri_400 + " == >" + local_path_400
        shutil.copyfile(video.uri_400, local_path_400)
        video.stateid = STATES['Video Downloaded']
        db_session.commit()
    except Exception, e:
        # TODO: Catch Other errors
        db_session.add(Logs(videoid, str(e)))
        video.stateid = STATES['Video Download Failed']
        db_session.commit()
    return


@app.task(base=SqlAlchemyTask)
def generate_clips(videoid):
    try:
        video = db_session.query(Videos).filter_by(id=videoid).first()
        print video
        server_folder = '/'.join(video.uri_1200.split("/")[:-1])
        video_path_1200 = os.path.join(os.getcwd(), "temp", video.showvideouuid, video.uri_1200.split("/")[-1])
        video_path_400 = os.path.join(os.getcwd(), "temp", video.showvideouuid, video.uri_400.split("/")[-1])
        lang = video.lang
        print video_path_400, video_path_1200
        # Find the total seconds first
        command = ["ffprobe", "-v", "error", "-show_entries", "format=duration",
                   "-of", "default=noprint_wrappers=1:nokey=1", video_path_400]
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        second_string = process.communicate()[0]
        seconds = int(float(str(second_string).strip()))
        if seconds < 11:
            # TODO: Save this error to DB
            db_session.add(Logs(videoid, "Video duration less than 10 secs."))
            return

        inpoint = seconds - 11
        outpoint = seconds - 1
        
        for video_path in [video_path_1200, video_path_400]:
            width = video_path.split('_')[-3].split('x')[0]
            height = video_path.split('_')[-3].split('x')[-1]
            bitrate = video_path.split('_')[-2]
            if 1.7 < float(width)/float(height) < 1.8:
                aspectratio = "16:9"
            elif 1.3 < float(width)/float(height) < 1.4:
                aspectratio = "4:3"
            else:
                # TODO: Hard coded to 1:1, may change later
                aspectratio = "1:1"

            # Create Videoasset
            destpath = video_path.split('.')[0] + "_10secs.mp4"
            if os.path.exists(destpath):
                os.remove(destpath)
            # Generate 10 seconds here
            video_command = ["ffmpeg", "-i", video_path, "-ss", str(inpoint), "-to", str(outpoint), "-an",
                            "-maxrate", "600k", "-bufsize", "1200k", "-profile:v", "baseline", "-level", "3.1",
                            "-f", "mp4", "-movflags", "+faststart", destpath]
            print video_command
            subprocess.Popen(video_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
            print "10 seconds clip generated"
            clip_uri = os.path.join(server_folder, destpath.split("/")[-1])
            if db_session.query(Videoassets).filter(Videoassets.videoid == videoid, Videoassets.width == width, 
                                                    Videoassets.height == height, Videoassets.uri == clip_uri).count():
                # TODO: should merge new fields with old entry?
                pass
            else:
                videoasset = Videoassets(videoid, clip_uri, width, height, bitrate, aspectratio, lang)
                db_session.add(videoasset)

            # Create Imageasset
            image_path = destpath.replace('mp4', 'jpg')
            if os.path.exists(image_path):
                os.remove(image_path)
            image_command = ["ffmpeg", "-i", destpath, "-ss", "0", "-vframes", "1", "-vcodec", "mjpeg", "-f",
                             "image2", image_path]
            subprocess.Popen(image_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()
            print "Thumbnail generated"
            image_uri = os.path.join(server_folder, image_path.split("/")[-1])
            # In case duplicates in Imageassets table
            if db_session.query(Imageassets).filter(Imageassets.videoid == videoid, Imageassets.width == width,
                                                    Imageassets.height == height, Imageassets.uri == image_uri).count():
                # TODO: should merge new fields with old entry?
                pass
            else:
                imageasset = Imageassets(videoid, image_uri, width, height, aspectratio)
                db_session.add(imageasset)

        video.stateid = STATES['Video Processed Successfully']
        db_session.commit()

    except subprocess.CalledProcessError:
        # TODO: Catch Subprocess errors
        print " Subprocess error "
        video.stateid = STATES['Ffmpeg Failed']
        db_session.commit()
        pass
    except SQLAlchemyError:
        # TODO: Catch SQLAlchemy errors
        pass
    except ValueError, e:
        # Will find "No such file or directory" need re-download
        # Delete the record and folder on disk, reprocess it from step 1
        folder_path = "temp/" + video.showvideouuid
        if os.path.exists(folder_path):
            shutil.rmtree(folder_path)
        logs = db_session.query(Logs).filter_by(videoid=videoid).all()
        if logs:
            for log in logs:
                print log.id
            db_session.delete(logs)
        db_session.delete(video)
        db_session.commit()
        pass
    except SoftTimeLimitExceeded:
        print "video: ", videoid, "exceed soft time limit"
    except Exception, e:
        print str(e)
        video.stateid = STATES['Ffmpeg Failed']
        db_session.commit()
    return


@app.task(base=SqlAlchemyTask)
def ftpput(videoid, folder_path):
    try:
        video = db_session.query(Videos).filter_by(id=videoid).first()
        ftp = FTP(CREDENTIAL["HOST"], CREDENTIAL["USER"], CREDENTIAL["PASS"])
        to_upload = []
        videoassets = db_session.query(Videoassets).filter_by(videoid=video.id).all()
        imageassets = db_session.query(Imageassets).filter_by(videoid=video.id).all()
        for videoasset in videoassets:
            to_upload.append(videoasset.uri)
        for imageasset in imageassets:
            to_upload.append(imageasset.uri)
        # print to_upload
        if to_upload and len(to_upload) == 4:
            for filepath in to_upload:
                filename = filepath.split("/")[-1]
                print "Uploading: " + os.path.join(folder_path, filename) + " == >" + filepath
                ftp.storbinary('STOR ' + filepath, open(os.path.join(folder_path, filename), 'rb'))
            video.stateid = STATES['Clip Uploaded']
            db_session.commit()
        else:
            # If not enough videoassets or imageassets, go back to state [Video Downloaded]
            video.stateid = STATES['Video Downloaded']
            db_session.commit()
        ftp.quit()

    except all_errors:
        # TODO: Catch FTP errors
        video.stateid = STATES['Clip Upload Failed']
        db_session.commit()
        pass
    except SQLAlchemyError:
        # TODO: Catch SQLAlchemy errors
        pass
    except:
        video.stateid = STATES['Clip Upload Failed']
        db_session.commit()
    return


@app.task(base=SqlAlchemyTask)
def localput(videoid, folder_path):
    try:
        video = db_session.query(Videos).filter_by(id=videoid).first()
        to_upload = []
        videoassets = db_session.query(Videoassets).filter_by(videoid=video.id).all()
        imageassets = db_session.query(Imageassets).filter_by(videoid=video.id).all()
        for videoasset in videoassets:
            to_upload.append(videoasset.uri)
        for imageasset in imageassets:
            to_upload.append(imageasset.uri)
        if to_upload and len(to_upload) == 4:
            for filepath in to_upload:
                filename = filepath.split("/")[-1]
                local_path = os.path.join(folder_path, filename)
                print "Uploading: " + local_path + " == >" + filepath
                shutil.copyfile(local_path, filepath)
            video.stateid = STATES['Clip Uploaded']
            db_session.commit()
        else:
            # If not enough videoassets or imageassets, go back to state [Video Downloaded]
            video.stateid = STATES['Video Downloaded']
            db_session.commit()
    except:
        video.stateid = STATES['Clip Upload Failed']
        db_session.commit()
    return


@app.task(base=SqlAlchemyTask)
def generate_xml(videoid, folder_path):
    try:
        video = db_session.query(Videos).filter_by(id=videoid).first()
        videoassets = db_session.query(Videoassets).filter_by(videoid=videoid).all()
        imageassets = db_session.query(Imageassets).filter_by(videoid=videoid).all()
        if len(videoassets) != 2 or len(imageassets) != 2:
            # TODO: Log this error
            db_session.add(Logs(videoid, "videoassets or imageassets not right"))
            video.stateid = STATES['Video Processed Successfully']
            imageassets_to_delete = db_session.query(Imageassets).filter_by(videoid=videoid).all()
            videoassets_to_delete = db_session.query(Videoassets).filter_by(videoid=videoid).all()
            db_session.delete(imageassets_to_delete)
            db_session.delete(videoassets_to_delete)
            db_session.commit()
            exit()
        arc_tree = ET.parse("template/arc_template.xml")
        root = arc_tree.getroot()

        root.find('arcnamespace').text = video.namespace
        # If no series uuid, delete the node
        if not video.seriesuuid:
            root.remove(root.find('seriesuuid'))
        else:
            root.find('seriesuuid').text = video.seriesuuid
        # If no episode uuid, delete the node
        if not video.episodeuuid:
            root.remove(root.find('episodeuuid'))
        else:
            root.find('episodeuuid').text = video.episodeuuid
        root.find('videoplaylistuuid').text = video.videoplaylistuuid
        root.find('showvideouuid').text = video.showvideouuid
        root.find('type').text = "showvideo"
        root.find('workflowtype').text = "posterclip"
        root.find('title').text = video.title
        root.find('duration').text = "10"
        root.find('lang').text = video.lang

        image = root.find('images').find('image')
        image.find('title').text = video.title
        image.find('imagetype').text = settings.ARCFormats["thumbnail"]
        imageassets_objs = image.find('imageassets').findall('imageasset')
        for i in xrange(2):
            if video.hostentry:
                imageassets_objs[i].find('imageuri').text = settings.FTP_ENTRY[video.hostentry]['MGID_PREFIX']+'/'\
                                                            +'/'.join(imageassets[i].uri.split('/')[2:])
            else:
                # TODO: other host entry
                imageassets_objs[i].find('imageuri').text = imageassets[i].uri
            imageassets_objs[i].find('width').text = str(imageassets[i].width)
            imageassets_objs[i].find('height').text = str(imageassets[i].height)
            imageassets_objs[i].find('format').text = settings.ARCFormats["jpg"]
            imageassets_objs[i].find('aspectratio').text = imageassets[i].aspectratio

        videoassets_objs = root.find('videoassets').findall('videoasset')
        for i in xrange(2):
            videoassets_objs[i].find('lang').text = videoassets[i].lang
            if video.hostentry:
                videoassets_objs[i].find('uri').text = settings.FTP_ENTRY[video.hostentry]['MGID_PREFIX']+'/'\
                                                       +'/'.join(videoassets[i].uri.split('/')[2:])
            else:
                # TODO: other host entry
                videoassets_objs[i].find('uri').text = videoassets[i].uri
            videoassets_objs[i].find('width').text = str(videoassets[i].width)
            videoassets_objs[i].find('height').text = str(videoassets[i].height)
            videoassets_objs[i].find('bitrate').text = str(videoassets[i].bitrate)
            videoassets_objs[i].find('format').text = settings.ARCFormats["mp4_h264_main"]
            videoassets_objs[i].find('duration').text = str(videoassets[i].duration)
            videoassets_objs[i].find('aspectratio').text = videoassets[i].aspectratio

        # print ET.tostring(root, encoding="utf-8")
        string_with_CDATA = addCDATA(ET.tostring(root, encoding="utf-8"))
        with open(folder_path+"/"+video.showvideouuid+".xml", 'wb') as w:
            w.write(string_with_CDATA.encode('utf8'))
        video.stateid = STATES['Xml Generated']
        db_session.commit()

    except SQLAlchemyError:
        # TODO: Catch SQLAlchemy errors
        pass
    except UnicodeDecodeError, e:
        video.stateid = STATES['Xml Generate Failed']
        db_session.add(Logs(videoid, str(e)))
        db_session.commit()
        raise
    except UnboundLocalError:
        #
        print "Could not find this video: ", videoid
    except Exception, e:
        print e
        video.stateid = STATES['Xml Generate Failed']
        db_session.commit()
    return


def X(data):
    """
    helper for handling unicode issues
    :param data:
    :return:
    """
    try:
        return data.decode('utf8', 'strict').encode('utf8')
    except UnicodeDecodeError:
        return data.decode('unicode_escape')
    except ValueError:
        return data.encode('utf8')
    except:
        return data


def addCDATA(xml_string):
    """
    helper for Adding CDATA tag in title
    :param xml_string:
    :return:
    """
    root = etree.fromstring(xml_string)
    title = root.find('./title')
    title.text = etree.CDATA(title.text)
    image_title = root.find('./images/image/title')
    image_title.text = etree.CDATA(image_title.text)

    return etree.tostring(root, encoding="unicode")
