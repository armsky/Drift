import os
from tasks_drift import ftpget, localcopy, generate_clips, ftpput, localput, generate_xml
import datetime
from socket import error as socket_error
import urllib2
import shutil
import json
from db_drift import db_session, Videos, Logs, STATES
from settings import FTP_ENTRY, MAX_DOWNLOADS, MAX_UPLOADS, MAX_FFMPEG, DOWNLOAD_UPLOAD_METHOD

__author__ = 'Hao Lin'


IDMO_PREFIX = "/8619/_!"
# query_header = "http://arcmig.mongo-arc-v2.mtvnservices.com/jp/mtvla.com?q="
query_header = "http://arcmig.mongo-arc-v2.mtvnservices.com/jp/nodp/mtv.com?q="
# query_header2 = "http://arcmig.mongo-arc-v2.mtvnservices.com/jp/ios.playplex.mtvla.com?q="
query_tailer = "&stage=live&dateFormat=UTC&indent=true&light=true"
SHARED_STORAGE_ARCMIG = "/arc/drift/"


def query_arc():
    """
    Query ARC database to get videos need to be processed.
    :return: None
    """
    with open("template/json_query_new.json", 'r') as json_file:
        json_string = json_file.read().replace(' ', '').replace('\n', '')
    whole_query = query_header + json_string + query_tailer
    print whole_query
    response = urllib2.urlopen(whole_query)
    json_object = json.loads(response.read())
    # print json.dumps(json_object)
    docs = json_object['response']['docs']
    for doc in docs:
        if 'Videos' in doc:
            # print doc['Episode']
            videos = doc['Videos']
            title = X(videos[0]['Title'])

            try:
                namespace = doc['mtvi:namespace']
                episodeuuid = doc['Episode']['mtvi:id']
                seriesuuid = doc['Series']['mtvi:id']
                videoplaylistuuid = doc['mtvi:id']
                showvideouuid = videos[0]['mtvi:id']
            except:
                # If any of these uuid or namespace is empty, ignore this video
                continue

            lang = doc['Language']
            # show video uuid should be unique in our database (each show will only have one poster video)
            # So if episode video uuid exists, skip this loop, not insert it as a new record
            if db_session.query(Videos).filter_by(episodeuuid=episodeuuid).count():
                continue
            if 'VideoAssetRefs' not in videos[0]:
                continue
            videorefs = videos[0]['VideoAssetRefs']
            for ref in videorefs:
                if ref['BitRate'] == '1200':
                    uri_1200 = ref['URI']
                elif ref['BitRate'] == '400':
                    uri_400 = ref['URI']
            # Only accepts videos from GSP for now
            if "mgid:file:gsp:" in uri_1200:
                try:
                    new_video = Videos(1, title, namespace, showvideouuid, videoplaylistuuid, episodeuuid, seriesuuid,
                                       uri_1200, uri_400, lang)
                    db_session.add(new_video)
                except UnboundLocalError:
                    print "uri_1200 or uri_400 might not be found"
                    print ref
                except:
                    raise
        else:
            print "Not a valid doc"
            print doc
    db_session.commit()


def download():
    """
    Create dir in temp folder and download video files
    :return: None
    """
    download_method = DOWNLOAD_UPLOAD_METHOD
    # Change the URI if needed, create temp folders in local disk
    videos = db_session.query(Videos).filter_by(stateid=STATES['Video Logged']).all()
    count = 0
    for video in videos:
        try:
            video_path_slices_1200 = video.uri_1200.split(':')
            if len(video_path_slices_1200) != 1:
                # GSP
                if video_path_slices_1200[2] == 'gsp':
                    if video_path_slices_1200[3] in [entry for entry in FTP_ENTRY.keys()]:
                        video.hostentry = video_path_slices_1200[3]

                        video.uri_1200 = FTP_ENTRY[video.hostentry]["FTP_PREFIX"] + video_path_slices_1200[4]
                        video.uri_400 = FTP_ENTRY[video.hostentry]["FTP_PREFIX"] + video.uri_400.split(':')[4]
                    else:
                        new_log = Logs(video.id, "host entry not recognized: " + video_path_slices_1200[3])
                        db_session.add(new_log)
                        db_session.commit()
                        print "host entry not recognized:", video_path_slices_1200[3]
                # else:
                #     # TODO: New GSP host entry, log to DB
                #     print video.uri_1200
                #     print "###", video.id, "Not Supported yet."

            folder_path = "temp/" + video.showvideouuid
            os.mkdir(folder_path)
            video.stateid = STATES['Video Ready To Download']
            count += 1
            if download_method == "FTP" and count >= 100:
                break
        except OSError:
            print "folder " + folder_path + " already exists... delete"
            # the state will stay at 1
            shutil.rmtree(folder_path)
            continue
        except IndexError:
            # TODO: something wrong with uri list
            continue
        except:
            raise
    db_session.commit()
    #  Download all videos with state=2
    videos = db_session.query(Videos).filter_by(stateid=STATES['Video Ready To Download']).all()
    for video in videos if len(videos) <= MAX_DOWNLOADS else videos[0:MAX_DOWNLOADS]:
        folder_path = "temp/" + video.showvideouuid
        if not os.path.exists(folder_path):
            video.stateid = STATES['Video Logged']
            db_session.commit()
        else:
            try:
                video.stateid = STATES['Video Downloading in Queue']
                db_session.commit()
                print video.id
                if download_method == "FTP":
                    ftpget.delay(video.id, folder_path)
                elif download_method == "MOUNT":
                    localcopy.delay(video.id, folder_path)
                else:
                    print "Not a proper way to download videos from GSP"
            except socket_error:
                # TODO: rabbitMQ not running or port not right, save to database
                print "RabbitMQ connection refused"
        # db_session.commit()


def process_video():
    upload_method = DOWNLOAD_UPLOAD_METHOD
    try:
        # Generate clips and thumbnails
        videos = db_session.query(Videos).filter_by(stateid=STATES['Video Downloaded']).all()
        for video in videos if len(videos) <= MAX_FFMPEG else videos[0:MAX_FFMPEG]:
            video.stateid = STATES['Ffmpeg Generating in Queue']
            db_session.commit()
            generate_clips.delay(video.id)
        # Upload clips and thumbnails
        videos = db_session.query(Videos).filter_by(stateid=STATES['Video Processed Successfully']).all()
        for video in videos if len(videos) <= MAX_UPLOADS else videos[0:MAX_UPLOADS]:
            folder_path = "temp/" + video.showvideouuid
            if not os.path.exists(folder_path):
                video.stateid = STATES['Clip Upload Failed']
                db_session.commit()
                print "Folder", folder_path, "not found with videoid=", video.id
            else:
                video.stateid = STATES['Clip Uploading in Queue']
                db_session.commit()
                if upload_method == "FTP":
                    ftpput.delay(video.id, folder_path)
                elif upload_method == "MOUNT":
                    localput.delay(video.id, folder_path)
                else:
                    print "Not a proper way to download videos from GSP"
    except socket_error:
        # TODO: rabbitMQ not running or port not right, save to database
        print "RabbitMQ connection refused"


def process_xml():
    try:
        # Generate Xml to ARC
        videos = db_session.query(Videos).filter_by(stateid=STATES['Clip Uploaded']).all()
        for video in videos:
            print "generate xml for video: ", video.id
            folder_path = "temp/" + video.showvideouuid
            video.stateid = STATES['Xml Generating in Queue']
            db_session.commit()
            generate_xml.delay(video.id, folder_path)
        # Copy Xml to shared storage
        videos = db_session.query(Videos).filter_by(stateid=STATES['Xml Generated']).all()
        if videos:
            time_string = datetime.datetime.now().strftime("%Y-%m-%d_%H_%M")
            batch_folder = os.path.join(SHARED_STORAGE_ARCMIG, time_string)
            if not os.path.exists(batch_folder):
                os.makedirs(batch_folder)
            for video in videos:
                xml_path = "temp/" + video.showvideouuid + "/" + video.showvideouuid + ".xml"
                dest_path = batch_folder + "/" + video.showvideouuid + ".xml"
                shutil.copy(xml_path, dest_path)
                video.stateid = STATES['Xml Ready To Send/Pickup']
            # Archive the XMLs in batch
            archive_batch_folder = os.path.join("archive", time_string)
            shutil.copytree(batch_folder, archive_batch_folder)
    except socket_error:
        # TODO: rabbitMQ not running or port not right, save to database
        print "RabbitMQ connection refused"
    except Exception, e:
        print str(e)
        video.stateid = STATES['Cleanup Failed']
        db_session.add(Logs(video.id, str(e)))

    db_session.commit()


def cleanup():
    videos = db_session.query(Videos).filter_by(stateid=STATES['ARC ID Retrieved']).all()
    for video in videos:
        try:
            # Clean up temp folder
            folder_path = "temp/" + video.showvideouuid
            shutil.rmtree(folder_path)
            video.stateid = STATES['Cleaned Up']
            print folder_path, " had been deleted"
        except Exception, e:
            video.stateid = STATES['Cleanup Failed']
            db_session.add(Logs(video.id, str(e)))

    db_session.commit()

    # Clean up XML folder


def fix():
    # Fix processes
    videos = db_session.query(Videos).filter_by(stateid=STATES['Xml Generating in Queue']).all()
    videos += db_session.query(Videos).filter_by(stateid=STATES['Xml Generate Failed']).all()
    for video in videos:
        video.stateid = STATES['Clip Uploaded']

    videos = db_session.query(Videos).filter_by(stateid=STATES['Clip Uploading in Queue']).all()
    videos += db_session.query(Videos).filter_by(stateid=STATES['Clip Upload Failed']).all()
    for video in videos:
        video.stateid = STATES['Video Processed Successfully']

    videos = db_session.query(Videos).filter_by(stateid=STATES['Ffmpeg Generating in Queue']).all()
    videos += db_session.query(Videos).filter_by(stateid=STATES['Ffmpeg Failed']).all()
    for video in videos:
        video.stateid = STATES['Video Downloaded']

    videos = db_session.query(Videos).filter_by(stateid=STATES['Video Downloading in Queue']).all()
    videos += db_session.query(Videos).filter_by(stateid=STATES['Video Download Failed']).all()
    for video in videos:
        video.stateid = STATES['Video Ready To Download']

    # Clean Logs table if the video had been fully processed
    logs = db_session.query(Logs).all()
    for log in logs:
        if log.video.stateid == STATES['Cleaned Up']:
            db_session.delete(log)

    db_session.commit()


def X(data):
    # Handle unicode issues
    try:
        return data.decode('utf8', 'strict').encode('utf8')
    except UnicodeDecodeError:
        return data.decode('unicode_escape')
    except ValueError:
        return data.encode('utf8')
    except:
        return data


# query_arc()
fix()
cleanup()
process_xml()
process_video()
# download()

