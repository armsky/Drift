from db_drift import Videos, db_session

__author__ = 'Hao Lin'

with open("template/p_mtv.csv") as csv:
    lines = csv.readlines()
    for one_line in lines[1:]:
        line = one_line.split(',')
        # print line
        title = line[3]
        namespace = line[4]
        showvideouuid = line[5]
        videoplaylistuuid = line[6]
        episodeuuid = line[7]
        seriesuuid = line[8]
        uri_1200 = line[9]
        uri_400 = line[10]
        lang = line[11]

        new_video = Videos(1, title, namespace, showvideouuid, videoplaylistuuid, episodeuuid,
                           seriesuuid, uri_1200, uri_400, lang)
        # print new_video.__dict__
        db_session.add(new_video)
    db_session.commit()
    print len(lines)
