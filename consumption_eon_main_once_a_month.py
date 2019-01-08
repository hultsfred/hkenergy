from pathlib import Path
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
    TABLE_CONSUMPTION_HOURLY,
)
from energi._PRIVATE_MAIL import MAILSERVER, FROM_, TO, SUBJECT
from energi._config import HEADLESS, SENDMAIL
from cryptography.fernet import Fernet
import pymssql

PW_DB = Fernet(KEY_DB).decrypt(PASSWORD_DB).decode("utf8")
PW_EON = Fernet(KEY).decrypt(PASSWORD_EON).decode("utf8")

YEAR = pendulum.now().subtract(months=1).year
MONTH = pendulum.now().subtract(months=1).month
FOLDER = "data_consumption"
FOLDER2 = "data_consumption_old"
DATASOURCE = "eon_consumption"
MESSAGEHEADER = "Eon consumption"
WORKINGDIR = "."
TRUNCATE = False
CONTROLDUPLICATES = False
headless = HEADLESS

CONN_LOG = pymssql.connect(SERVER, USER_DB, PW_DB, DB)
CURSOR_LOG = CONN_LOG.cursor()


@exception(
    create_logger_db(
        CONN_LOG, CURSOR_LOG, TABLE_LOG, "energi_eon_consumption_once_a_month"
    )
)
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
        en.eon_consumption(hourly=True)
        data = en.eon_consumption_transform()
        # print(en.period)
        # en.db_delete_records(
        #    server=SERVER,
        #    database=DB,
        #    table=TABLE_CONSUMPTION_HOURLY,
        #    user=USER_DB,
        #    password=PW_DB,
        #    whereClause=f"""YEAR(DATEADD(hh, -1,Timestamp)) = {en.year}
        #                    AND MONTH(DATEADD(hh, -1,Timestamp)) = {en.month}""",
        # )
        # en.db_insert(
        #    data=data,
        #    server=SERVER,
        #    db=DB,
        #    table=TABLE_CONSUMPTION_HOURLY,
        #    user=USER_DB,
        #    pw=PW_DB,
        #    truncate=TRUNCATE,
        # )
        en.clean_folder(destinationFolder=FOLDER2)
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
