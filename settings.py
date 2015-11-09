__author__ = 'Hao Lin'

# Database options are: 'DBA_LIVE', 'DBA_QA', 'DBA_DEV', 'LOCALHOST', '204'
DATABASE_OPTION = "DBA_LIVE"

# Download & upload method options are: 'FTP', 'MOUNT'
DOWNLOAD_UPLOAD_METHOD = "MOUNT"

FTP_CREDENTIAL = {
    "HOST": "gsprelay.viacom.com",
    "USER": "linh",
    "PASS": "IPepsi543",
}

FTP_ENTRY = {
    "alias": {
        "FTP_PREFIX":   "/GSPstor/gsp-alias",
        "MGID_PREFIX":  "mgid:file:gsp:alias:",
        "LOCAL_MOUNT":  "/GSPstor/gsp-alias"
    },
    "cmtstor":      {
        "FTP_PREFIX":   "/GSPstor/cmtstor",
        "MGID_PREFIX":  "mgid:file:gsp:cmtstor:"
    },
    "egvrenditions":      {
        "FTP_PREFIX":   "/GSPstor/egvrenditions",
        "MGID_PREFIX":  "mgid:file:gsp:egvrenditions:"
    },
    "logostor":     {
        "FTP_PREFIX":   "/GSPstor/logostor",
        "MGID_PREFIX":  "mgid:file:gsp:logostor:"
    },
    "mtvcomstor":   {
        "FTP_PREFIX":   "/GSPstor/mtvcomstor",
        "MGID_PREFIX":  "mgid:file:gsp:mtvcomstor:"
    },
    "originmusicstor":    {
        "FTP_PREFIX":   "/GSPstor/originmusicstor",
        "MGID_PREFIX":  "mgid:file:gsp:originmusicstor:"
    },
    "vhonecomstor":    {
        "FTP_PREFIX":   "/GSPstor/vhonecomstor",
        "MGID_PREFIX":  "mgid:file:gsp:vhonecomstor:"
    },
    "mtviestor":     {
        "HOST":     "mtviestor.upload.akamai.com",
        "USER":     "mtviestor_scripting",
        "PASS":     "mtviestor_scripting1",
        "FTP_PREFIX":   "/8619/_!",
        "PREFIX":       "mgid:file:gsp:mtviestor:/"
    },
    # "GSP_MTVN": {
    #     "FTP_PREFIX":   "",
    #     "PREFIX": "mgid:file:gsp:mtvnintlstor:/"
    # },

}

ARCFormats = {
    "thumbnail":        "645fd648-9697-47c0-b01e-415e4f2b4e45",
    "jpg":              "1ae36ce7-2b9e-4c03-a26b-dabe940de56a",
    "mp4_h264_main":    "787e4200-a2fa-102e-b731-0026b9419ed3"
}

# Limit the thread in Celery
MAX_DOWNLOADS = 100
MAX_UPLOADS = 100
MAX_FFMPEG = 10
