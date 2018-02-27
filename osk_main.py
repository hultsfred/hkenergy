from energi.energi_osk import ENERGI_OSK
from cryptography.fernet import Fernet
from energi._PRIVATE_XL import PATH, TABLE
from energi._PRIVATE_DB import SERVER, DB, USER_DB, PASSWORD_DB, KEY_DB

if __name__ == '__main__':
    PW_DB = Fernet(KEY_DB).decrypt(PASSWORD_DB).decode('utf8')
    osk = ENERGI_OSK(path=PATH)
    data = osk.getFact()
    #print(osk.columns)
    #print(data)
    osk.db_insert(
                data=data,
                server=SERVER,
                db=DB,
                table=TABLE,
                user=USER_DB,
                pw=PW_DB)