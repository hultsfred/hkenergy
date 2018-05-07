from energi.energi import Energi
import pendulum
from hkfunctions.api import exception, create_logger_db, send_mail
import traceback
from energi._PRIVATE_ENERGI import PASSWORD_LOS, KEY, LOS, USER
from energi._PRIVATE_DB import SERVER, DB, TABLE_LOG, USER_DB, PASSWORD_DB, KEY_DB, TABLE_LOS
from energi._config import HEADLESS
from cryptography.fernet import Fernet
import pymssql

MAILSERVER = 'hks-mgw.hultsfred.se'
FROM_ = 'energi_integration@hultsfred.se'
TO = 'henric.sundberg@hultsfred.se'
SUBJECT = 'Fel i energiintegrationen'

PW_DB = Fernet(KEY_DB).decrypt(PASSWORD_DB).decode('utf8')
PW_LOS = Fernet(KEY).decrypt(PASSWORD_LOS).decode('utf8')

YEAR = pendulum.now().subtract(days=2).year
MONTH = pendulum.now().subtract(months=1).month
FOLDER = 'data_cost'
FOLDER2 = 'data_cost_old'
DATASOURCE = 'los'
MESSAGEHEADER = 'LOS'
WORKINGDIR = '.'
TRUNCATE = False
headless = HEADLESS

CONN_LOG = pymssql.connect(SERVER, USER_DB, PW_DB, DB)
CURSOR_LOG = CONN_LOG.cursor()


@exception(create_logger_db(CONN_LOG, CURSOR_LOG, TABLE_LOG, 'energi_los'))
def main():
    try:
        en = Energi(
            workingDir=WORKINGDIR,
            adress=LOS,
            user=USER,
            pw=PW_LOS,
            downloadPath=FOLDER,
            year=YEAR,
            month=MONTH,
            datasource=DATASOURCE,
            headless=headless)
        en.los_cost()
        data = en.los_cost_transform()
        en.db_delete_records(
            server=SERVER,
            database=DB,
            table=TABLE_LOS,
            user=USER_DB,
            password=PW_DB,
            whereClause=f"""Period = '{en.period[:4]+'-'+en.period[4:]}'""",
        )
        en.db_insert(
            data=data,
            server=SERVER,
            db=DB,
            table=TABLE_LOS,
            user=USER_DB,
            pw=PW_DB,
            truncate=TRUNCATE,
        )
        en.clean_folder()
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
