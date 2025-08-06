# backend/config.py
import os

class Config:
    DEBUG = True
    SECRET_KEY = os.environ.get('SECRET_KEY', 'dev_secret')

    SQLALCHEMY_DATABASE_URI = os.environ.get(
        'DATABASE_URL',
        'mysql+pymysql://u782952718_eee:Vanrodolf123.@srv667.hstgr.io/u782952718_eee'
    )

    SQLALCHEMY_TRACK_MODIFICATIONS = False
