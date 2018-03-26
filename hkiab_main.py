from energi.energi_xl import ENERGI_XL
from cryptography.fernet import Fernet
from energi._PRIVATE_XL import PATH_HKIAB_META, TABLE_HKIAB_META
from energi._PRIVATE_DB import SERVER, DB, USER_DB, PASSWORD_DB, KEY_DB

if __name__ == '__main__':
    PW_DB = Fernet(KEY_DB).decrypt(PASSWORD_DB).decode('utf8')
    hkiab = ENERGI_XL(path=PATH_HKIAB_META)
    data = hkiab.getHkiabMeta()
    hkiab.db_insert(
        data=data,
        server=SERVER,
        db=DB,
        table=TABLE_HKIAB_META,
        user=USER_DB,
        pw=PW_DB,
        truncate='yes')
