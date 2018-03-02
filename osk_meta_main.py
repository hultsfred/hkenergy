from energi.energi_xl import ENERGI_XL
from cryptography.fernet import Fernet
from energi._PRIVATE_XL import PATH_OSK_META, TABLE_OSK_META
from energi._PRIVATE_DB import SERVER, DB, USER_DB, PASSWORD_DB, KEY_DB

if __name__ == '__main__':
    PW_DB = Fernet(KEY_DB).decrypt(PASSWORD_DB).decode('utf8')
    osk = ENERGI_XL(path=PATH_OSK_META)
    data = osk.getOskMeta()
    #print(data)
    osk.db_insert(
                data=data,
                server=SERVER,
                db=DB,
                table=TABLE_OSK_META,
                user=USER_DB,
                pw=PW_DB,
                truncate='yes')
