import pandas as pd
import numpy as np
from hkfunctions.api import mssql_insert
from typing import Union, List, Tuple, Any, Optional

PATH = './mall/test_Drift_2018.xlsx'  # korrigera denna, sätt korrekt path

class ENERGI_OSK():
    """
    this class consists of methods that are used to extract, wrangle and insert data into a db from an excelfile in which
    ÖSK enters energy data 
    """

    def __init__(self, path: str) -> None:
        """
        * If new medias, i.e. new columns, are added in the excefile, the column names must be added to MEDIA and _TYPE_MEDIA.
          WATCH THE ORDER OF _TYPE_MEDIA. It much match the order of the inenr lists in MEDIA
        * If new columns are added to a existing media, these columns must be added to the correspondinng inner list in
          MEDIA.
        """
        self.path = path
        self.sheet_FactData: str = 'Sammanställning'
        self.sheet_ElectricityMeta = ''
        self.sheet_EstateMeta = ''
        self.sheet_SubsidiaryEstateMeta = ''
        self.MEDIA: list = [
            ['Fjärrvärme: Mätarställning Mwh', 'Fjärrvärme: Flöde'],
            ['Flis: Lev. m3'],
            ['Vatten: Mätarställning m3', 'Vatten: Typ'],
            ['El: Mätarställning kwh'],
            ['Olja: Liter', 'Olja: Typ'],
            ['Pellets: Avläsning'],
            ['Briketter: Avläsning'],
        ]
        self._TYPE_MEDIA: list = [
            'Fjärrvärme',
            'Flis',
            'Vatten',
            'El',
            'Olja',
            'Pellets',
            'Briketter',
        ]
        self._columns: list = [
            'År',
            'Period',
            'ObjektId',
            'Id',
            'Förbrukning',            
            'Media',
            'Typ',
        ]
        self.general_columns: list = [
            'År',
            'Period',
            'ObjektId',
        ]
        self.extra_id_vars: dict = {
            'Vatten': ['Vatten: Typ'],
            'Olja': ['Olja: Typ'],
        }

    def getFact(self) -> List[Tuple[Any, ...]]:
        """
        
        """
        xl = pd.ExcelFile(self.path)
        data = xl.parse(self.sheet_FactData)
        data.drop(
            [
                'Månad',
                'Objektnamn',
            ], axis=1, inplace=True)
        df = pd.DataFrame(columns=self._columns)
        for i, j in zip(self.MEDIA, self._TYPE_MEDIA):
            cols = self.general_columns + i
            _df = data[cols]
            if j != 'Vatten' and j != 'Olja':
                __df = pd.melt(
                    _df,
                    id_vars=self.general_columns,
                    var_name='Id',
                    value_name='Förbrukning')
            else:
                __df = pd.melt(
                    _df,
                    id_vars=self.general_columns + self.extra_id_vars[j],
                    var_name='Id',
                    value_name='Förbrukning')
                __df = __df.rename(columns={self.extra_id_vars[j][0]: 'Typ'})
            __df['Media'] = j
            __df.dropna(inplace=True)
            df = df.append(__df)
        _str = ':'
        df.Id = df.Id.apply(
            lambda i: i[i.find(_str) + 2:])  # tar bort onödig text
        df = df[self._columns].reset_index(
            drop=True
        )  # en bugg i pandas hgör att kolumnerna kastas om när append används, detta korrigerar detta
        df['Förbrukning'] = np.round(df['Förbrukning'].astype(float), 2)
        fact = [tuple(i) for i in df.values.tolist()]
        return fact

    def getElectricityMeta(self) -> List[Tuple[Any, ...]]:
        """

        """
        pass

    def getEstateMeta(self) -> List[Tuple[Any, ...]]:
        """

        """
        pass

    def getSubsidiaryEstateMeta(self) -> List[Tuple[Any, ...]]:
        """

        """
        pass

    def db_insert(self,
                  data: List[Tuple[Any, ...]],
                  server: str,
                  db: str,
                  table: str,
                  user: str,
                  pw: str,
                  truncate: str = 'no') -> None:
        """
        insert data into database        
        """
        mssql_insert(
            server,
            db,
            table=table,
            data=data,
            user=user,
            password=pw,
            truncate=truncate)


if __name__ == '__main__':
    osk = ENERGI_OSK(path=PATH)
    print(osk.getFact())
    print(osk._columns)