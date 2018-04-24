from energi.energi_xl import ENERGI_XL
from cryptography.fernet import Fernet
import traceback
from energi._PRIVATE_XL import PATH_OSK_CONSUMPTION, TABLE_OSK_ESTATE_META, TABLE_OSK_ELECTRICITY_META, TABLE_OSK_SUBSIDIARY_ESTATE_META
from energi._PRIVATE_DB import SERVER, DB, USER_DB, PASSWORD_DB, KEY_DB, TABLE_LOG
from hkfunctions.api import exception, create_logger_db, send_mail
import pymssql

MAILSERVER = 'hks-mgw.hultsfred.se'
FROM_ = 'energi_integration@hultsfred.se'
TO = 'henric.sundberg@hultsfred.se'
SUBJECT = 'Fel i ÖSK energiintegration'
DATASOURCE = 'ÖSK consumption'
MESSAGEHEADER = 'ÖSK'

PW_DB = Fernet(KEY_DB).decrypt(PASSWORD_DB).decode('utf8')

CONN_LOG = pymssql.connect(SERVER, USER_DB, PW_DB, DB)
CURSOR_LOG = CONN_LOG.cursor()


@exception(create_logger_db(CONN_LOG, CURSOR_LOG, TABLE_LOG, 'energi_ÖSK'))
def main():
    try:
        osk = ENERGI_XL(path=PATH_OSK_CONSUMPTION)
        #print(osk.getElectricityMeta())
        electricityMeta = osk.getMeta(osk.sheet_ElectricityMeta)
        estateMeta = osk.getMeta(osk.sheet_EstateMeta)
        subsidiaryEstateMeta = osk.getMeta(osk.sheet_SubsidiaryEstateMeta)
        data = [
            electricityMeta,
            estateMeta,
            subsidiaryEstateMeta,
        ]
        tables = [
            TABLE_OSK_ELECTRICITY_META,
            TABLE_OSK_ESTATE_META,
            TABLE_OSK_SUBSIDIARY_ESTATE_META,
        ]
        for d, t in zip(data, tables):
            try:
                osk.db_insert(
                    data=d,
                    server=SERVER,
                    db=DB,
                    table=t,
                    user=USER_DB,
                    pw=PW_DB,
                    truncate='yes')
            except pymssql.ProgrammingError:
                continue
    except:
        #send_mail(
        #    MAILSERVER,
        #    FROM_,
        #    TO,
        #    SUBJECT,
        #    messageHeader=MESSAGEHEADER,
        #    messageBody=traceback.format_exc())
        raise


if __name__ == '__main__':
    main()
