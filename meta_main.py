from energi.energi import Energi
import pendulum
from hkfunctions.log import exception, create_logger_db
from hkfunctions.mail import send_mail
import traceback
from energi._PRIVATE_ENERGI import PASSWORD_EON, KEY, EON, USER
from energi._PRIVATE_DB import (
    SERVER,
    DB,
    TABLE_LOG,
    USER_DB,
    PASSWORD_DB,
    KEY_DB,
    TABLE_META,
)
from energi._PRIVATE_MAIL import MAILSERVER, FROM_, TO, SUBJECT
from energi._config import HEADLESS, SENDMAIL
from cryptography.fernet import Fernet
import pymssql

PW_DB = Fernet(KEY_DB).decrypt(PASSWORD_DB).decode("utf8")
PW_EON = Fernet(KEY).decrypt(PASSWORD_EON).decode("utf8")

YEAR = pendulum.now().subtract(days=5).year
MONTH = pendulum.now().subtract(months=1).month
FOLDER = "data_meta"
FOLDER2 = "data_meta_old"
DATASOURCE = "eon_meta"
MESSAGEHEADER = "Meta"
WORKINGDIR = "."
TRUNCATE = True
CONTROLDUPLICATES = True
headless = HEADLESS

CONN_LOG = pymssql.connect(SERVER, USER_DB, PW_DB, DB)
CURSOR_LOG = CONN_LOG.cursor()


@exception(create_logger_db(CONN_LOG, CURSOR_LOG, TABLE_LOG, "energi_meta"))
def main():
    try:
        en = Energi(
            workingDir=WORKINGDIR,
            adress=EON,
            user=USER,
            pw=PW_EON,
            downloadPath=FOLDER,
            year=YEAR,
            month=MONTH,
            datasource=DATASOURCE,
            headless=headless,
        )
        en.meta()
        data = en.meta_transform()
        en.db_insert(
            data=data,
            server=SERVER,
            db=DB,
            table=TABLE_META,
            user=USER_DB,
            pw=PW_DB,
            truncate=TRUNCATE,
        )
        en.clean_folder()
    except Exception:
        if SENDMAIL:
            send_mail(
                MAILSERVER,
                FROM_,
                TO,
                SUBJECT,
                messageHeader=MESSAGEHEADER,
                messageBody=traceback.format_exc(),
            )
        raise


if __name__ == "__main__":
    main()
