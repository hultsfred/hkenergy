import pandas as pd
import numpy as np
from hkfunctions.api import mssql_insert
from typing import Union, List, Tuple, Any, Optional


class ENERGI_XL():
    """ this class consists of methods that are used to extract, wrangle and insert data into a db from excelfiles in which
    ÖSK enters energy data, HKIAB enters metadata
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
        self.columns: list = [
            'Period',
            'ObjektId',
            'Media',
            'Typ',
            'Information',
            'Enhet',
            'Förbrukning',
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
        """gets data from the given excel file and transform it
        
        :return: A 
        :rtype: list
        """

        xl = pd.ExcelFile(self.path, convertes={'År': str, 'Period': str})
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
        df['Förbrukning'] = np.round(df['Förbrukning'].astype(float), 2)
        df['År'] = df['År'].astype(int).astype(str)
        df['Period'] = df['År'] + '-' + df['Period'].astype(int).astype(
            str).apply(lambda x: x if len(x) == 2 else '0' + x)
        df.drop('År', axis=1, inplace=True)
        df['ObjektId'] = df['ObjektId'].astype(int).astype(str)
        df['Enhet'] = df['Id'].apply(lambda i: i[i.find(' ') + 1:].lower())
        df['Information'] = df['Id'].apply(
            lambda i: i[:i.find(' ') + 1].lower() if len(i[:i.find(' ') + 1]) > 0 else None
        )
        df = df.where(pd.notnull(df), None)
        df = df[self.columns].reset_index(drop=True)
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

    def getHkiabMeta(self) -> List[Tuple[Any, ...]]:
        """[summary]
        
        """
        df = pd.read_excel(self.path, skiprows=9)
        df = df[['Anläggnings id', 'Fastighet', 'Adress',
                 'Anmärkning']].dropna(
                     axis=0, how="all")
        df['Anläggnings id'] = df['Anläggnings id'].astype(str).apply(
            lambda x: x.replace(' ', ''))
        df = df.where(pd.notnull(df), None)
        fact = [tuple(i) for i in df.values.tolist()]
        return fact

    def getOskMeta(self) -> List[Tuple[Any, ...]]:
        """[summary]
        
        :return: [description]
        :rtype: List[Tuple[Any, ...]]
        """
        CONVERTERS = {
            'Säkring': str,
            'Referens 1': str,
            'Elområde': str,
            'Faktura GLN': str,
            'Kndnr E.ON (nät)': str,
        }
        df = pd.read_excel(self.path, converters=CONVERTERS)
        df['Anläggningsid'] = df['Anläggningsid'].astype(str).apply(
            lambda x: x.replace(' ', ''))
        ortid = df['Avdelning'].apply(lambda x: x[x.find('-')+2:])
        df['Avdelning'] = df['Avdelning'].apply(lambda x: x[:x.find('-')-1])
        df.insert(loc=1, column='Ortid', value=ortid)
        df = df.where(pd.notnull(df), None)
        fact = [tuple(i) for i in df.values.tolist()]
        return fact

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
