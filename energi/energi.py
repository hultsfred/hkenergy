import os
import pathlib
from pathlib import Path
from decimal import Decimal
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.common.exceptions import (NoSuchElementException,
                                        UnexpectedAlertPresentException,
                                        InvalidElementStateException)
from selenium.webdriver.common.keys import Keys
import time
import pandas as pd
from hkfunctions.api import mssql_insert, mssql_query
from energi._eon_info import years
from typing import Union, List, Tuple, Any, Optional
ts = time.sleep


class Energi():
    """
    This class includes methods to help download exceldata from EON and LOS
    websites.
    """
    driverPath: str = r'.\webdriver\geckodriver.exe'

    def __init__(self, workingDir: str, **kwargs) -> None:
        """

        """
        os.chdir(workingDir)
        self.adress: str = kwargs.pop('adress', None)
        self.user: str = kwargs.pop('user', None)
        self.pw: str = kwargs.pop('pw', None)
        if not kwargs.pop('fullDownloadPath', False):
            self.downloadPath: str = str(
                Path.cwd() / kwargs.pop('downloadPath', None))
        else:
            self.downloadPath: str = kwargs.pop('downloadPath', None)
        if not os.path.isdir(self.downloadPath):
            raise IOError(f'{self.downloadPath} is not a correct path.')
        self.year: int = kwargs.pop('year', None)
        self.month: int = kwargs.pop('month', None)
        self.datasource = kwargs.pop('datasource', None)
        headless = kwargs.pop('headless', True)
        if kwargs:
            raise KeyError(
                f'The keyword/s {list(kwargs.keys())} is/are incorrect.')
        ######### Firefox profile
        fp = webdriver.FirefoxProfile()
        fp.set_preference("browser.preferences.instantApply", True)
        fp.set_preference(
            "browser.helperApps.neverAsk.saveToDisk",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet, application/octet-stream",
        )
        fp.set_preference("browser.download.folderList", 2)
        fp.set_preference("browser.download.dir", self.downloadPath)
        fp.set_preference("browser.helperApps.alwaysAsk.force", False)
        fp.set_preference("browser.download.manager.showWhenStarting", False)
        opts = Options()
        if headless:
            opts.set_headless()
        self.opts: Options = opts
        self.firefox_profile: webdriver.FirefoxProfile = fp
        #########

    def eon_consumption(self) -> None:
        """Downloads file from EON"""
        _year: int = self.year
        month: int = self.month
        year: int = years[_year]
        try:
            driver = webdriver.Firefox(
                executable_path=self.driverPath,
                firefox_profile=self.firefox_profile,
                options=self.opts)
            driver.get(self.adress)
            ts(5)
            elem1 = driver.find_element_by_id("UserIdField")
            elem2 = driver.find_element_by_id("PasswordField")
            elem1.send_keys(self.user)
            ts(5)
            elem2.send_keys(self.pw)
            # logga in
            driver.find_element_by_id("LoginButton").click()
            ts(5)
            # väl hultsfreds kommun
            driver.find_element_by_class_name('business-account-link').click()
            ts(3)
            # välj analys
            driver.find_element_by_id("installation-box").click()
            ts(5)
            # välj alla fastigheter
            driver.find_element_by_xpath(
                'id("installations-result-tab")/table[@class="table table-hover"]/thead[1]/tr[1]/th[@class="no-padding col-md-2"]/button[@class="add-to btn btn-outline with-icon icon-add-to add-all-facilities width-100 no-border"]'
            ).click()
            ts(5)
            # applicera förändringarna
            driver.find_element_by_id("apply-changes").click()
            ts(10)
            # gör val
            #välj månad
            driver.find_element_by_xpath(
                '//*[@id="period-from-month"]').click()  # val av från period
            ts(30)
            driver.find_element_by_xpath(
                '/html/body/div[6]/div[2]/table/thead/tr/th[2]').click(
                )  #val av år
            ts(5)
            driver.find_element_by_xpath(
                f'/html/body/div[6]/div[3]/table/tbody/tr/td/span[{year}] '
            ).click()  # 9=2017
            ts(3)
            driver.find_element_by_xpath(
                f'/html/body/div[6]/div[2]/table/tbody/tr/td/span[{month}]'
            ).click()  #val av månad 11=nov
            ts(3)
            driver.find_element_by_xpath(
                '//*[@id="period-to-month"]').click()  # val av tom perid
            ts(3)
            driver.find_element_by_xpath(
                '/html/body/div[6]/div[2]/table/thead/tr/th[2]').click(
                )  #val av år
            ts(3)
            driver.find_element_by_xpath(
                f'/html/body/div[6]/div[3]/table/tbody/tr/td/span[{year}]'
            ).click()  # 9=2017
            ts(3)
            driver.find_element_by_xpath(
                f'/html/body/div[6]/div[2]/table/tbody/tr/td/span[{month}]'
            ).click()  # val av månad 11=nov
            ts(3)
            driver.find_element_by_xpath('//*[@id="fetch-data"]').click(
            )  # bekräfta föränringrana av data
            ts(5)
            #övriga val
            driver.find_element_by_xpath(
                '//*[@id="display-type-compare"]/span').click()  # välj jämför
            #ts(3)
            #driver.find_element_by_xpath('//*[@id="lbl-show-temp"]/span').click() #välj visa temp, verkat ointe som dett aföljer med till excel, isf onödigt, förstör valet av jämför också
            ts(5)
            #driver.find_element_by_xpath('//*[@id="scope"]/form/div[8]/div/div/label/span').click() # avgruppera månadfsvis
            ts(10)
            driver.find_element_by_xpath(
                '//*[@id="consumption-timeframe"]/div/div/div/div[1]/div/button/span[1]'
            ).click(
            )  #välj tidintervall, kostander går endast att få om valet är månader
            ts(5)
            #driver.find_element_by_xpath('//*[@id="consumption-cost-header-tab"]/a') # fö att få kostander, används denna så går valet av tidsinetrbvall tillbala till månad
            #ts(5)
            driver.find_element_by_xpath(
                '//*[@id="consumption-timeframe"]/div/div/div/div[1]/div/div/ul/li[5]/a/span'
            ).click()  # välj timvärden
            ts(5)
            # klikcka på öppna i
            driver.find_element_by_xpath(
                '//*[@id="actions"]/form/div[1]/div[1]/div/div[1]/div/button'
            ).click()
            ts(5)
            # välj öppna i excel
            driver.find_element_by_xpath(
                'id("actions")/form[1]/div[@class="row"]/div[@class="col-sm-12"]/div[@class="row"]/div[@class="col-sm-6 i-chart-types"]/div[@class="btn-group dropdown open"]/div[@class="dropdown-menu pull-right no-border"]/button[@class="export btn btn-outline width-100"]'
            ).click()
            #waits for file to download
            self._check_folder(self.downloadPath)
            #ts(60)
            driver.find_element_by_class_name("log-out").click()
            ts(3)
            driver.close()
        except (NoSuchElementException, InvalidElementStateException):
            try:
                driver.find_element_by_class_name("log-out").click()
            except (NoSuchElementException, InvalidElementStateException):
                pass
            driver.close()
            raise

    def eon_cost(self):
        """

        """
        year = self.year
        month = self.month
        year = years[year]
        try:
            driver = webdriver.Firefox(
                executable_path=self.driverPath,
                firefox_profile=self.firefox_profile)
            driver.get(self.adress)
            ts(5)
            elem1 = driver.find_element_by_id("UserIdField")
            elem2 = driver.find_element_by_id("PasswordField")
            elem1.send_keys(self.user)
            ts(5)
            elem2.send_keys(self.pw)
            # logga in
            driver.find_element_by_id("LoginButton").click()
            ts(5)
            # väl hultsfreds kommun
            driver.find_element_by_class_name('business-account-link').click()
            ts(3)
            # välj ekonomi
            driver.find_element_by_xpath(
                '//*[@id="body"]/div[2]/div/div[2]/ul/li[4]').click()
            ts(3)
            # välj kostnader
            driver.find_element_by_xpath(
                '//*[@id="body"]/div[2]/div/div[2]/ul/li[4]/ul/li[2]/a').click(
                )
            ts(10)
            # välj startmånad
            driver.find_element_by_id('dateFrom').click()
            ts(3)
            driver.find_element_by_xpath(
                '/html/body/div[6]/div[2]/table/thead/tr/th[2]').click(
                )  #val av år
            ts(3)
            driver.find_element_by_xpath(
                f'/html/body/div[6]/div[3]/table/tbody/tr/td/span[{year}]'
            ).click()  # 9=2017
            ts(3)
            driver.find_element_by_xpath(
                f'/html/body/div[6]/div[2]/table/tbody/tr/td/span[{month}]'
            ).click()
            ts(3)
            # välj slutmånad
            driver.find_element_by_id('dateTo').click()
            ts(3)
            driver.find_element_by_xpath(
                '/html/body/div[6]/div[2]/table/thead/tr/th[2]').click(
                )  #val av år
            driver.find_element_by_xpath(
                f'/html/body/div[6]/div[3]/table/tbody/tr/td/span[{year}]'
            ).click()  # 9=2017
            ts(3)
            driver.find_element_by_xpath(
                f'/html/body/div[6]/div[2]/table/tbody/tr/td/span[{month}]'
            ).click()
            ts(3)
            driver.find_element_by_xpath(
                '//*[@id="costs-filter"]/div[6]/div/button').click()
            ts(10)
            driver.find_element_by_xpath(
                'id("list-container")/div[@class="base-list"]/table[@class="table table-hover view14 sorted-7 sorted-desc"]/thead[@class="tableFloatingHeaderOriginal"]/tr[@class="header"]/th[@class="select"]/label[1]/span[1]'
            ).click()
            ts(5)
            driver.find_element_by_xpath(
                '//*[@id="list-container"]/div[1]/div/div/button').click()
            ts(5)
            driver.find_element_by_xpath(
                '//*[@id="list-container"]/div[1]/div/div/div/button[1]'
            ).click()
            # väntar på att fil laddar ner
            Energi._check_folder(self.downloadPath)
            #ts(15)
            driver.find_element_by_class_name("log-out").click()
            ts(3)
            driver.close()
        except (NoSuchElementException, InvalidElementStateException):
            try:
                driver.find_element_by_class_name("log-out").click()
            except (NoSuchElementException, InvalidElementStateException):
                pass
            driver.close()
            raise

    def los_cost(self):
        """
        
        """
        if self.month < 10:
            m = str('0' + str(self.month))
            from_ = str(self.year) + '-' + m
            to = str(self.year) + '-' + m
        else:
            from_ = str(self.year) + '-' + str(self.month)
            to = str(self.year) + '-' + str(self.month)
        try:
            driver = webdriver.Firefox(
                executable_path=self.driverPath,
                firefox_profile=self.firefox_profile,
                options=self.opts)
            driver.get(self.adress)
            ts(5)
            elem1 = driver.find_element_by_id("Login1_UserName")
            elem2 = driver.find_element_by_id("Login1_Password")
            elem1.send_keys(self.user)
            ts(3)
            elem2.send_keys(self.pw)
            # logga in
            driver.find_element_by_id("Login1_LoginButton").click()
            ts(15)
            # ange start slut period
            start = driver.find_element_by_id("txtFrom")
            end = driver.find_element_by_id("txtTo")
            start.clear()
            ts(3)
            end.clear()
            ts(3)
            start.send_keys(from_)
            ts(3)
            end.send_keys(to)
            ts(3)
            driver.find_element_by_id('btnReload').click()
            ts(3)
            # välj excelfil
            driver.find_element_by_id('lnkExport').click()
            #waits for file to download
            Energi._check_folder(self.downloadPath)
            #ts(5)
            # logga ut
            driver.find_element_by_xpath(
                '//*[@id="leftmenu"]/ul/li[8]/a').click()
            ts(5)
            driver.close()
        except Exception as exc:
            driver.find_element_by_xpath(
                '//*[@id="leftmenu"]/ul/li[8]/a').click()
            driver.close()
            print(exc)
            raise

    def eon_consumption_transform(self) -> List[Tuple[Any, ...]]:
        """
        transforms data downloaded by method eon_consumption to fit database.
        """
        p: Path = Path(self.downloadPath)
        _fileList: List[Path] = list(p.glob('**/*.xlsx'))
        if len(_fileList) > 1:
            raise IOError(
                f'There are more than one file in {self.downloadPath}')
        try:
            _file: Path = _fileList[0]
        except IndexError:
            raise IOError(
                f'The folder {self.downloadPath} is empty. Is there an error when downloading the file?'
            )
        cols_eon = pd.read_excel(_file, header=12).columns.tolist()
        cols_eon[0] = "Timestamp"
        data_eon = pd.read_excel(_file, skiprows=17, header=None)
        data_eon = data_eon.drop(0, 1)  #tar bort tom kolumn
        data_eon.columns = cols_eon
        data_eon = data_eon.dropna(axis=1, how='all')
        data_eon = pd.melt(
            data_eon,
            id_vars='Timestamp',
            var_name='Anläggningsnummer',
            value_name='Förbrukning')
        data_eon.dropna(inplace=True)  # blir det rätt här?
        data = [tuple(i) for i in data_eon.values.tolist()]
        return data

    def eon_cost_transform(self):
        """
        transforms eon cost data
        """
        p = Path(self.downloadPath)
        file = list(p.glob('**/*.xlsx'))
        if len(file) > 1:
            raise IOError(
                f'There are more than one file in {self.downloadPath}')
        try:
            file = file[0]
        except IndexError:
            raise IOError(
                f'The folder {self.downloadPath} is empty. Is there an error when downloading the file?'
            )
        eon_cost = pd.read_excel(file, skiprows=9, header=None)
        eon_cost.drop(labels=0, axis=1, inplace=True)  #tar bort tom kolu
        eon_cost.columns = eon_cost.iloc[0, :].tolist()
        eon_cost.drop(labels=0, axis=0, inplace=True)
        eon_cost[['Nät', 'Energi',
                  'Energiskatt']] = eon_cost[['Nät', 'Energi',
                                              'Energiskatt']].astype(float)
        eon_cost['Nät inkl moms'] = (eon_cost['Nät'] * 1.25).round(2)
        eon_cost['Energi inkl moms'] = (eon_cost['Energi'] * 1.25).round(2)
        eon_cost['Energiskatt inkl moms'] = (
            eon_cost['Energiskatt'] * 1.25).round(2)
        eon_cost['Summa inkl moms'] = (
            eon_cost['Summa'] * 1.25).astype(float).round(2)
        column_order = [
            'Anläggnings-ID', 'Förbrukningsperiod', 'Referens', 'Status',
            'Nät inkl moms', 'Energi inkl moms', 'Energiskatt inkl moms',
            'Summa inkl moms', 'Nät', 'Energi', 'Energiskatt', 'Varav Moms',
            'Summa'
        ]
        eon_cost = eon_cost[column_order]
        eon_cost = eon_cost.rename(
            columns={
                'Summa': 'Summa exkl moms',
                'Varav Moms': 'Moms',
                'Nät': 'Nät exkl moms',
                'Energi': 'Energi exkl moms',
                'Energiskatt': 'Energiskatt exkl moms',
            })
        eon_cost = pd.melt(
            eon_cost,
            id_vars=[
                'Anläggnings-ID', 'Förbrukningsperiod', 'Referens', 'Status'
            ],
            var_name='Typ',
            value_name='Kostnad')
        eon_cost = eon_cost[pd.notnull(eon_cost['Kostnad'])]
        eon_cost = eon_cost.where(pd.notnull(eon_cost), None)
        data = [tuple(i) for i in eon_cost.values.tolist()]
        return data

    def meta(self):
        """
        downloads metadata from eon
        """
        try:
            driver = webdriver.Firefox(
                executable_path=self.driverPath,
                firefox_profile=self.firefox_profile,
                options=self.opts)
            driver.get(self.adress)
            ts(5)
            #time.sleep(2)
            elem1 = driver.find_element_by_id("UserIdField")
            elem2 = driver.find_element_by_id("PasswordField")
            elem1.send_keys(self.user)
            ts(5)
            elem2.send_keys(self.pw)
            # logga in
            driver.find_element_by_id("LoginButton").click()
            ts(5)
            # välj hultsfreds kommun
            driver.find_element_by_class_name('business-account-link').click()
            ts(3)
            # välj Energikarta
            driver.find_element_by_xpath(
                '//*[@id="body"]/div[2]/div/div[2]/ul/li[2]').click()
            ts(5)
            driver.find_element_by_xpath(
                '//*[@id="body"]/div[2]/div/div[2]/ul/li[2]/ul/li[1]/a').click(
                )
            ts(5)
            driver.find_element_by_xpath(
                '//*[@id="installation-search-container"]/div[2]/div[1]/div/label[3]'
            ).click()
            ts(5)
            driver.find_element_by_xpath(
                '//*[@id="search-container"]/div/div/div/div/form/div/div[4]/button'
            ).click()
            ts(5)
            driver.find_element_by_xpath(
                'id("installation-list-tbl")/thead[@class="tableFloatingHeaderOriginal"]/tr[@class="header"]/th[@class="select"]/label[1]/span[1]'
            ).click()
            ts(5)
            driver.find_element_by_id('test1').click()
            ts(5)
            driver.find_element_by_xpath(
                '//*[@id="installation-list-container"]/div[1]/div[1]/div[1]/div/button[1]'
            ).click()
            #waits for file to download
            Energi._check_folder(self.downloadPath)
            #ts(15)
            driver.find_element_by_class_name("log-out").click()
            ts(3)
            driver.close()
        except NoSuchElementException:
            try:
                driver.find_element_by_class_name("log-out").click()
            except NoSuchElementException:
                pass
            driver.close()
            raise

    def meta_transform(self):
        """

        """
        p = Path(self.downloadPath)
        file = list(p.glob('**/*.xlsx'))
        if len(file) > 1:
            raise IOError(
                f'There are more than one file in {self.downloadPath}')
        try:
            file = file[0]
        except IndexError:
            raise IOError(
                f'The folder {self.downloadPath} is empty. Is there an error when downloading the file?'
            )
        meta_eon = pd.read_excel(
            file, skiprows=9, converters={
                'Anläggnings-ID ': str
            })
        meta_eon.dropna(axis=1, how='all', inplace=True)
        #meta_eon = meta_eon[meta_eon['Mätmetod '] == 'Tim']
        column_order = [
            'Anläggnings-ID ', 'Ort ', 'Elområde ', 'Nät-ID ',
            'Anläggningsadress ', 'Företag ', 'Organisationsnummer ',
            'Anläggningstyp ', 'Aktiv ', 'Mätmetod ', 'Nätprislista ',
            'Nätägare ', 'Säkringsnivå (A) ', 'Centralbet. ',
            'Betald ansl.kap (kW) '
        ]
        meta_eon = meta_eon[column_order]
        meta_eon = meta_eon.where(pd.notnull(meta_eon), None)
        data = [tuple(i) for i in meta_eon.values.tolist()]
        return data

    def los_cost_transform(self):
        """
        data transformation of los excel file. 
        """
        p = Path(self.downloadPath)
        _file = list(p.glob('**/*.xls*'))
        if len(_file) > 1:
            raise IOError(
                f'There are more than one file in {self.downloadPath}')
        try:
            _file = _file[0]
        except IndexError:
            raise IOError(
                f'The folder {self.downloadPath} is empty. Is there an error when downloading the file?'
            )
        df = pd.read_excel(_file, header=None)
        period = df.iloc[0, 0][7:13]
        period = period[:4] + '-' + period[4:]
        data = df.iloc[2:, :]
        data.insert(0, 'Period', period)
        cols = [
            'Period', 'Kundnamn', 'Org_nr', 'Anläggningsadress',
            'Anläggningsid', 'Nätområde', 'Elområde', 'Nätägare',
            'Ediel-ID Nätägare', 'Beräknad årsförbrukning', 'Mätmetod',
            'Avräkningsmetod', 'Anläggningstyp', 'Avläsningsdatum1',
            'Mätarställning1', 'Avläsningsdatum2', 'Mätarställning2',
            'LOS Energy-nummer', 'Startdatum', 'Slutdatum', 'Frifält 1 Objekt',
            'Frifält 2 Referens', 'Frifält 3 Kontosträng', 'Kundnummer',
            'Månadsförbr (kWh)', 'Månadsförbr avser', 'Volym kWh', 'Nät',
            'Arvode nät', 'Energi kr', 'Handelsavgifter (kr)',
            'Prissäkring (kr)', 'Arvode (kr)', 'Effektreserv (kr)',
            'Miljöel (kr)', 'Elcertifikat (kr)', 'Elcertifikat arvode (kr)',
            'Övrigt (kr)', 'Elskatt (kr)', 'Summa (kr)', 'Moms (kr)'
        ]
        data.columns = cols
        data.reset_index(drop=True, inplace=True)
        data = data.copy()
        data['Avläsningsdatum1'] = pd.to_datetime(
            data['Avläsningsdatum1'], yearfirst=True).astype(str)
        data['Avläsningsdatum2'] = pd.to_datetime(
            data['Avläsningsdatum2'], yearfirst=True).astype(str)
        data['Startdatum'] = pd.to_datetime(
            data['Startdatum'], yearfirst=True).astype(str)
        data['Slutdatum'] = data['Slutdatum'].apply(lambda x: (str(x)[:10]))
        data['Kundnummer'] = data['Kundnummer'].astype(
            str
        )  # must be set as a string, else pymssql tries to insert it as int
        data['LOS Energy-nummer'] = data['LOS Energy-nummer'].astype(str)
        data['Org_nr'] = data['Org_nr'].astype(str)
        data['Ediel-ID Nätägare'] = data['Ediel-ID Nätägare'].astype(str)
        data_cl = data.where(pd.notnull(data), None)
        data = [tuple(i) for i in data_cl.values.tolist()]
        return data

    def db_insert(self,
                  data: List[Tuple[Any, ...]],
                  server: str,
                  db: str,
                  table: str,
                  user: str,
                  pw: str,
                  truncate: bool = False,
                  controlForDuplicates: bool = True) -> None:
        """
        inserts data into database        
        """
        _truncate: str
        query: Optional[str] = None
        if truncate:
            _truncate = 'yes'
        else:
            _truncate = 'no'
        if controlForDuplicates:
            query = self._control_for_duplicates(
                server=server,
                db=db,
                table=table,
                data=data,
                user=user,
                password=pw,
            )
        if query:
            current_data = mssql_query(
                server=server, db=db, user=user, password=pw,
                query=query)  # bugg här
            if self.datasource == 'eon_consumption':
                time_format = '%Y-%m-%d %H:%M:%S'
                current_data = set((i[0].strftime(time_format), str(i[1]),
                                    float(i[2])) for i in current_data)
            else:
                _current_data = []
                for i in current_data:
                    _temp = []
                    for y in i:
                        if type(y) is Decimal:
                            y = float(y)
                        _temp.append(y)
                    _current_data.append(tuple(_temp))
                current_data = set(_current_data)
            data_diff = set(data) - current_data
            data = list(data_diff)
        if data:
            mssql_insert(
                server=server,
                db=db,
                table=table,
                data=data,
                user=user,
                password=pw,
                truncate=_truncate)

    def _help_db_insert(self):
        """

        """
        # TODO: skriv en metod som returnar 2 queries om start och end skiljer sig
        pass

    def clean_folder(self,
                     destinationFolder: str = None,
                     fullPath: bool = False) -> None:
        """
        Deletes all files in given dir or moves them to another dir.
        To move to another dir ther parameter destinationFolder must be specified.
        """
        p: Path
        path_content: List[Path]
        d: str = destinationFolder
        destination_path: Path
        p = Path(self.downloadPath)
        path_content = list(p.glob('**/*.xls*'))
        if path_content:
            for f in path_content:
                if not d:
                    os.remove(p / f)
                else:
                    path = p / f
                    destination_path = path.parent.parent / d / path.name
                    path.rename(destination_path)

    @staticmethod
    def _check_folder(folder: str) -> None:
        """
        controls if folder is empty. Raises error if time exceeds 60 seconds
        """
        _folder: Path = Path(folder)
        timeout = time.time() + 60 * 2  # 2 minutes
        while True:
            if time.time() > timeout:
                break  #
            path_content = list(_folder.glob('**/*.xls*'))
            if not path_content:
                ts(5)
                continue
            else:
                #print(time.time() - (timeout - 120))
                break

    def _control_for_duplicates(self, data: List[Tuple[Any, ...]], server: str,
                                db: str, table: str, user: str,
                                password: str) -> Optional[str]:
        """
        controls table if data already exists
        """
        if self.datasource == 'eon_consumption':
            query = f'SELECT * FROM {table} WHERE YEAR([Timestamp])={self.year} AND MONTH([TIMESTAMP])={self.month}'
        elif self.datasource == 'eon_cost':
            query = None
        elif self.datasource == 'eon_meta':
            query = None
        elif self.datasource == 'los':
            if self.month < 10:
                m = str('0' + str(self.month))
                period = str(self.year) + '-' + m
            else:
                period = str(self.year) + '-' + str(self.month)
            query = f"SELECT * FROM {table} WHERE [Period]='{period}';"
        else:
            query = None
        return query