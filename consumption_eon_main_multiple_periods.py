from pathlib import Path
from energi.energi import Energi
import pendulum
from hkfunctions.api import exception, create_logger_db, send_mail
import traceback
from energi._PRIVATE_ENERGI import PASSWORD_EON, KEY, EON, USER
from energi._PRIVATE_DB import SERVER, DB, TABLE_LOG, USER_DB, PASSWORD_DB, KEY_DB, TABLE_CONSUMPTION
from energi._PRIVATE_MAIL import MAILSERVER, FROM_, TO, SUBJECT
from energi._config import HEADLESS
from cryptography.fernet import Fernet
import pymssql

PW_DB = Fernet(KEY_DB).decrypt(PASSWORD_DB).decode('utf8')
PW_EON = Fernet(KEY).decrypt(PASSWORD_EON).decode('utf8')

YEAR = [2017, 2018]
MONTH = [
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8,
    9,
    10,
    11,
    12,
]
FOLDER = 'data_consumption'
FOLDER2 = 'data_consumption_old'
DATASOURCE = 'eon_consumption'
MESSAGEHEADER = 'Eon consumption'
WORKINGDIR = '.'
TRUNCATE = False
CONTROLDUPLICATES = True
headless = HEADLESS

CONN_LOG = pymssql.connect(SERVER, USER_DB, PW_DB, DB)
CURSOR_LOG = CONN_LOG.cursor()


@exception(
    create_logger_db(CONN_LOG, CURSOR_LOG, TABLE_LOG,
                     'energi_eon_consumption'))
def main():
    for y in YEAR:
        for m in MONTH:
            try:
                en = Energi(
                    workingDir=WORKINGDIR,
                    adress=EON,
                    user=USER,
                    pw=PW_EON,
                    downloadPath=FOLDER,
                    year=y,
                    month=m,
                    datasource=DATASOURCE,
                    headless=headless)
                en.eon_consumption()
                data = en.eon_consumption_transform()
                #print(data)
                en.db_insert(
                    data=data,
                    server=SERVER,
                    db=DB,
                    table=TABLE_CONSUMPTION,
                    user=USER_DB,
                    pw=PW_DB,
                    truncate=TRUNCATE,
                    controlForDuplicates=CONTROLDUPLICATES)
                en.clean_folder(destinationFolder=FOLDER2)
            except Exception:
                send_mail(
                    MAILSERVER,
                    FROM_,
                    TO,
                    SUBJECT,
                    messageHeader=MESSAGEHEADER,
                    messageBody=traceback.format_exc())
                raise


if __name__ == '__main__':
    main()