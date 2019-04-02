import os
import pathlib
from pathlib import Path
from decimal import Decimal
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.common.exceptions import (
    NoSuchElementException,
    UnexpectedAlertPresentException,
    InvalidElementStateException,
    ElementClickInterceptedException,
)
from selenium.webdriver.common.keys import Keys
import time
import pandas as pd
from hkfunctions.sqlserver import insert as mssql_insert
from energi._eon_info import years
from typing import Union, List, Tuple, Any, Optional, Dict

ts = time.sleep


class Energi:
    """
    This class includes methods to help download exceldata from EON and LOS
    websites.
    """

    def __init__(self, workingDir: str, **kwargs) -> None:
        """initializes the class
        
        :param workingDir: working directory
        :type workingDir: str
        :param **kwargs: different key value pair
        :type **kwargs: str, int
        :raises IOError: wrong downloadPath is given
        :raises KeyError: Incorrect keyword/s har given
        """
        self._driverPath: str = r".\webdriver\geckodriver.exe"
        os.chdir(workingDir)
        self._adress: str = kwargs.pop("adress", None)
        self._user: str = kwargs.pop("user", None)
        self._pw: str = kwargs.pop("pw", None)
        if not kwargs.pop("full_downloadPath", False):
            self._downloadPath: str = str(Path.cwd() / kwargs.pop("downloadPath", None))
        else:
            self._downloadPath: str = kwargs.pop("downloadPath", None)
        if not os.path.isdir(self._downloadPath):
            raise IOError(f"{self._downloadPath} is not a correct path.")
        self._year: int = kwargs.pop("year", None)
        self._month: int = kwargs.pop("month", None)
        self._tertial: bool = kwargs.pop("tertial", None)
        self._datasource: str = kwargs.pop("datasource", None)
        headless: bool = kwargs.pop("headless", True)
        if kwargs:
            raise KeyError(f"The keyword/s {list(kwargs.keys())} is/are incorrect.")
        ######### Firefox profile
        fp = webdriver.FirefoxProfile()
        fp.set_preference("browser.preferences.instantApply", True)
        fp.set_preference(
            "browser.helperApps.neverAsk.saveToDisk",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet, application/octet-stream, text/csv",
        )
        fp.set_preference("browser.download.folderList", 2)
        fp.set_preference("browser.download.dir", self._downloadPath)
        fp.set_preference("browser.helperApps.alwaysAsk.force", False)
        fp.set_preference("browser.download.manager.showWhenStarting", False)
        opts = Options()
        if headless:
            opts.set_headless()
        self._opts: Options = opts
        self._firefox_profile: webdriver.FirefoxProfile = fp
        #########

    def log_in_eon(self) -> None:
        """[summary]
        
        :return: [description]
        :rtype: None
        """
        driver = webdriver.Firefox(
            executable_path=self._driverPath,
            firefox_profile=self._firefox_profile,
            options=self._opts,
        )
        driver.get(self._adress)
        ts(5)
        elem1 = driver.find_element_by_id("UserIdField")
        elem2 = driver.find_element_by_id("PasswordField")
        elem1.send_keys(self._user)
        ts(5)
        elem2.send_keys(self._pw)
        # logga in
        driver.find_element_by_id("LoginButton").click()
        ts(5)
        return driver

    

    @staticmethod
    def choose_start_period_eon(driver, year, month, calling_function) -> None:
        """[summary]
        
        :return: [description]
        :rtype: None
        """
        if calling_function == "consumption":
            _id = "period-from-month"
        elif calling_function == "cost":
            _id = "dateFrom"
        driver.find_element_by_id(_id).click()  # val av start period
        ts(3)
        driver.find_element_by_xpath(
            "/html/body/div[7]/div[2]/table/thead/tr/th[2]"
        ).click()  # val av år
        ts(3)
        driver.find_element_by_xpath(
            f"/html/body/div[7]/div[3]/table/tbody/tr/td/span[{year}]"
        ).click()  # 9=2017
        ts(3)
        driver.find_element_by_xpath(
            f"/html/body/div[7]/div[2]/table/tbody/tr/td/span[{month}]"
        ).click()  # val av månad 11=nov
        return driver

    @staticmethod
    def choose_end_period_eon(driver, year, month, calling_function):
        """[summary]
        
        :param driver: [description]
        :type driver: [type]
        :param year: [description]
        :type year: [type]
        :param month: [description]
        :type month: [type]
        """
        if calling_function == "consumption":
            _id = "period-to-month"
        elif calling_function == "cost":
            _id = "dateTo"
        driver.find_element_by_id(_id).click()  # val av start period
        ts(3)
        driver.find_element_by_xpath(
            "/html/body/div[7]/div[2]/table/thead/tr/th[2]"
        ).click()  # val av år
        ts(3)
        driver.find_element_by_xpath(
            f"/html/body/div[7]/div[3]/table/tbody/tr/td/span[{year}]"
        ).click()  # 9=2017
        ts(3)
        driver.find_element_by_xpath(
            f"/html/body/div[7]/div[2]/table/tbody/tr/td/span[{month}]"
        ).click()  # val av månad 11=nov
        if calling_function == "cost":
            # sök => applicera valda månader
            driver.find_element_by_xpath(
                '//*[@id="costs-filter"]/div[6]/div/button'
            ).click()
            ts(10)
        return driver

    @staticmethod
    def hourly_eon(driver):
        """[summary]
        
        :param driver: [description]
        :type driver: [type]
        """
        driver.find_element_by_xpath(
            '//*[@id="consumption-timeframe"]/div/div/div/div[1]/div/button/span[1]'
        ).click()  # välj tidintervall, kostander går endast att få om valet är månader
        ts(5)
        # driver.find_element_by_xpath('//*[@id="consumption-cost-header-tab"]/a') # fö att få kostander, används denna så går valet av tidsinetrbvall tillbala till månad
        # ts(5)
        driver.find_element_by_xpath(
            '//*[@id="consumption-timeframe"]/div/div/div/div[1]/div/div/ul/li[6]/a/span'
        ).click()  # välj timvärden
        ts(20)
        return driver

    @staticmethod
    def log_out_eon(driver):
        """[summary]
        
        :param driver: [description]
        :type driver: [type]
        """
        driver.find_element_by_class_name("log-out").click()
        ts(3)
        driver.close()

    @staticmethod
    def choose_analysis_all_facilities(driver):
        """[summary]
        
        :param driver: [description]
        :type driver: [type]
        """
        # välj analys
        driver.find_element_by_id("installation-box").click()
        ts(5)
        # välj alla fastigheter
        driver.find_element_by_css_selector(".add-all-facilities").click()
        ts(5)
        # applicera förändringarna
        driver.find_element_by_css_selector("#apply-changes").click()
        ts(7)
        return driver

    @staticmethod
    def download_excel_consumption_eon(driver):
        """[summary]
        
        :param driver: [description]
        :type driver: [type]
        """
        # klikcka på öppna i
        driver.find_element_by_xpath(
            '//*[@id="actions"]/form/div[1]/div[1]/div/div[1]/div/button'
        ).click()
        ts(5)
        # välj öppna i excel
        driver.find_element_by_xpath(
            'id("actions")/form[1]/div[@class="row"]/div[@class="col-sm-12"]/div[@class="row"]/div[@class="col-sm-6 i-chart-types"]/div[@class="btn-group dropdown open"]/div[@class="dropdown-menu pull-right no-border"]/button[@class="export btn btn-outline width-100"]'
        ).click()
        return driver

    @staticmethod
    def choose_economics_costs_eon(driver):
        """[summary]
        
        :param driver: [description]
        :type driver: [type]
        """
        # välj ekonomi
        driver.find_element_by_xpath(
            '//*[@id="body"]/div[2]/div/div[2]/ul/li[4]'
        ).click()
        ts(3)
        # välj kostnader
        driver.find_element_by_xpath(
            '//*[@id="body"]/div[2]/div/div[2]/ul/li[4]/ul/li[2]/a'
        ).click()
        ts(10)
        return driver

    @staticmethod
    def choose_all_download_excel_cost_eon(driver):
        """[summary]
        
        :param driver: [description]
        :type driver: [type]
        """
        # välj alla fastigheter
        driver.find_element_by_css_selector(
            "#list-container > div.base-list > table > thead.tableFloatingHeaderOriginal > tr > th.select > label > span"
        ).click()
        ts(5)
        # öpnna menyn "Öppna i..."
        driver.find_element_by_css_selector(
            "#list-container > div.row.base-list-toolbar > div > div > button"
        ).click()
        ts(5)
        # klicka ladda ner
        driver.find_element_by_css_selector(
            "#list-container > div.row.base-list-toolbar > div > div > div > button.export.btn.btn-outline.width-100"
        ).click()
        return driver

    @staticmethod
    def choose_compare_consumption_eon(driver):
        """[summary]
        
        :param driver: [description]
        :type driver: [type]
        """
        driver.find_element_by_css_selector(
            "#display-type-compare > span:nth-child(2)"
        ).click()  # välj jämför
        ts(10)
        return driver

    @staticmethod
    def choose_company_eon(driver):
        """[summary]
        
        :param driver: [description]
        :type driver: [type]
        """
        # välj hultsfreds kommun
        # driver.find_element_by_class_name('business-account-link').click() # HK och HKIAB har bytt plats därför måste css selector användas
        order = 2
        driver.find_element_by_css_selector(
            f"body > form > div > div > div.modal-body > div.form-group > table > tbody > tr:nth-child({order}) > td > div > a"
        ).click()
        ts(3)
        return driver

    def eon_consumption(self, hourly: bool = False) -> None:
        """Downloads file from EON"""
        _year: int = self.year
        month: int = self.month
        year: int = years[_year]
        try:
            driver = self.log_in_eon()
            try:
                driver = self.choose_company_eon(driver)
            except NoSuchElementException:
                pass
            driver = self.choose_analysis_all_facilities(driver)
            driver = self.choose_start_period_eon(driver, year, month, "consumption")
            driver = self.choose_end_period_eon(driver, year, month, "consumption")
            driver = self.choose_compare_consumption_eon(driver)
            if hourly:
                driver = self.hourly_eon(driver)
            driver = self.download_excel_consumption_eon(driver)
            # waits for file to download
            self._check_folder(self._downloadPath)
            # ts(60)
            self.log_out_eon(driver)
        except (NoSuchElementException, InvalidElementStateException):
            try:
                driver.find_element_by_class_name("log-out").click()
            except (
                NoSuchElementException,
                InvalidElementStateException,
                ElementClickInterceptedException,
            ):
                pass
            driver.close()
            raise

    def eon_cost(self):
        """downloads file with costs from eon
        """
        if not self._tertial:
            year = years[self.year]
            month = self.month
        else:
            _tertial = self._createTertial()
            startYear = years[int(_tertial[0][:4])]
            endYear = years[int(_tertial[3][:4])]
            startMonth = int(_tertial[0][5:])
            endMonth = int(_tertial[3][5:])
        try:
            driver = self.log_in_eon()
            try:
                driver = self.choose_company_eon(driver)
            except NoSuchElementException:
                pass
            driver = self.choose_economics_costs_eon(driver)
            if not self._tertial:
                driver = self.choose_start_period_eon(driver, year, month, "cost")
                driver = self.choose_end_period_eon(driver, year, month, "cost")
            else:
                # välj startmånad
                driver = self.choose_start_period_eon(
                    driver, startYear, startMonth, "cost"
                )
                driver = self.choose_end_period_eon(driver, endYear, endMonth, "cost")
            driver = self.choose_all_download_excel_cost_eon(driver)
            # väntar på att fil laddar ner
            self._check_folder(self._downloadPath)
            # ts(15)
            self.log_out_eon(driver)
        except (NoSuchElementException, InvalidElementStateException):
            try:
                driver.find_element_by_class_name("log-out").click()
            except (
                NoSuchElementException,
                InvalidElementStateException,
                ElementClickInterceptedException,
            ):
                pass
            driver.close()
            raise

    def login_los(self):
        driver = webdriver.Firefox(
            executable_path=self._driverPath,
            firefox_profile=self._firefox_profile,
            options=self._opts,
        )
        driver.get(self._adress)
        ts(5)
        elem1 = driver.find_element_by_id("Login1_username")
        elem2 = driver.find_element_by_id("Login1_password")
        elem1.send_keys(self._user)
        ts(3)
        elem2.send_keys(self._pw)
        # logga in
        driver.find_element_by_id("Login1_LoginButton").click()
        ts(15)
        return driver

    @staticmethod
    def logout_los(driver):
        """[summary]
        
        :param driver: [description]
        :type driver: [type]
        """
        driver.find_element_by_xpath(
            "/html/body/form/div[4]/header/div/div/ul/li[2]/div/div/a[1]"
        ).click()
        ts(5)
        driver.close()

    def los_cost(self):
        """Downloads costs from los
        
        """

        if self.month < 10:
            m = str("0" + str(self.month))
            from_ = str(self.year) + "-" + m
            to = str(self.year) + "-" + m
        else:
            from_ = str(self.year) + "-" + str(self.month)
            to = str(self.year) + "-" + str(self.month)
        try:
            driver = self.login_los()
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
            driver.find_element_by_id("btnReload").click()
            ts(30)
            # välj excelfil
            driver.find_element_by_id("lnkExport").click()
            # waits for file to download
            self._check_folder(self._downloadPath)
            # ts(5)
            # logga ut
            self.logout_los(driver)
        except Exception as exc:
            self.logout_los(driver)
            print(exc)
            raise

    def neova_consumption(self):
        """Downloads costs from neova
        
        """
        try:
            driver = webdriver.Firefox(
                executable_path=self._driverPath,
                firefox_profile=self._firefox_profile,
                options=self._opts,
            )
            driver.get(self._adress)
            ts(5)
            elem1 = driver.find_element_by_name("email")
            elem2 = driver.find_element_by_name("password")
            elem1.send_keys(self._user)
            ts(3)
            elem2.send_keys(self._pw)
            # logga in
            driver.find_element_by_class_name("auth0-label-submit").click()
            ts(15)
            self._neova_helper(driver)
            driver.find_element_by_xpath(
                '//*[@id="app"]/div/div/div[2]/div/div[2]/button'
            ).click()
            driver.close()
        except Exception:
            driver.close()
            raise

    def _neova_helper(self, driver: webdriver) -> None:
        """[summary]
        """
        # farligt med att namnge filerna utifrpn ordningen i listan på webben. denna kan ändras. Bättre om metadata fanns i csvfilen.
        neova_anläggningar: Dict = {
            "Mimer_7": '//*[@id="app"]/div/div/div[1]/div[2]/div[1]/nav/div/div[2]/div/ul/li[2]/div[2]',
            "Läroverket_Stålhagskolan": '//*[@id="app"]/div/div/div[1]/div[2]/div[1]/nav/div/div[2]/div/ul/li[3]/div[2]',
            "Smeden_8": '//*[@id="app"]/div/div/div[1]/div[2]/div[1]/nav/div/div[2]/div/ul/li[4]/div[2]',
            "Solvändan_2": '//*[@id="app"]/div/div/div[1]/div[2]/div[1]/nav/div/div[2]/div/ul/li[5]/div[1]',
            "Läroverket_C_huset": '//*[@id="app"]/div/div/div[1]/div[2]/div[1]/nav/div/div[2]/div/ul/li[6]/div[2]',
        }

        for key, value in neova_anläggningar.items():
            driver.find_element_by_class_name("usage-place-bar__button").click()
            ts(5)
            driver.find_element_by_xpath(value).click()
            ts(5)
            driver.find_element_by_class_name("usage-place-menu__close").click()
            ts(2)
            # ladda ner csv
            driver.find_element_by_xpath(
                '//*[@id="base__content"]/div[2]/div[1]/div[7]/a'
            ).click()
            ts(5)
            self._rename_file(facility=key)

    def eon_consumption_transform(self) -> List[Tuple[Any, ...]]:
        """transforms data downloaded by method eon_consumption to fit database.
        
        """
        p: Path = Path(self._downloadPath)
        _fileList: List[Path] = list(p.glob("**/*.xlsx"))
        if len(_fileList) > 1:
            raise IOError(f"There are more than one file in {self._downloadPath}")
        try:
            _file: Path = _fileList[0]
        except IndexError:
            raise IOError(
                f"The folder {self._downloadPath} is empty. Is there an error when downloading the file?"
            )
        cols_eon = pd.read_excel(_file, header=12).columns.tolist()
        cols_eon = cols_eon[1:]
        cols_eon[0] = "Timestamp"
        data_eon = pd.read_excel(_file, skiprows=19, header=None)
        data_eon = data_eon.drop(0, 1)  # tar bort tom kolumn
        data_eon.columns = cols_eon
        data_eon = data_eon.dropna(axis=1, how="all")
        data_eon = pd.melt(
            data_eon,
            id_vars="Timestamp",
            var_name="Anläggningsnummer",
            value_name="Förbrukning",
        )
        data_eon.dropna(inplace=True)  # blir det rätt här?
        data = [tuple(i) for i in data_eon.values.tolist()]
        return data

    def eon_consumption_transform_monthly(self) -> List[Tuple[Any, ...]]:
        """[summary]
        
        Returns:
            List[Tuple[Any, ...]] -- [description]
        """

        p: Path = Path(self._downloadPath)
        _fileList: List[Path] = list(p.glob("**/*.xlsx"))
        if len(_fileList) > 1:
            raise IOError(f"There are more than one file in {self._downloadPath}")
        try:
            _file: Path = _fileList[0]
        except IndexError:
            raise IOError(
                f"The folder {self._downloadPath} is empty. Is there an error when downloading the file?"
            )
        data = pd.read_excel(_file, header=6, sheet_name="Data")
        period = data.iloc[5, 0]
        data = data.transpose().reset_index(drop=True)
        columns = data.iloc[0, :].tolist()
        columns = [c if c != period else "Förbrukning" for c in columns]
        data.columns = columns
        data = data.drop(labels=0, axis=0).rename(columns={"Tidpunkt": "Period"})
        data.Period = period.replace("-", "")
        data["Anläggnings-ID"] = data["Anläggnings-ID"].astype(str)
        data.Förbrukning = data.Förbrukning.astype(float)
        data = data.where(pd.notnull(data), None)
        data = [tuple(i) for i in data.values.tolist()]
        return data

    def eon_cost_transform(self):
        """transforms eon cost data
        
        """
        p = Path(self._downloadPath)
        file = list(p.glob("**/*.xlsx"))
        if len(file) > 1:
            raise IOError(f"There are more than one file in {self._downloadPath}")
        try:
            file = file[0]
        except IndexError:
            raise IOError(
                f"The folder {self._downloadPath} is empty. Is there an error when downloading the file?"
            )
        eon_cost = pd.read_excel(file, skiprows=9, header=None)
        eon_cost.drop(labels=0, axis=1, inplace=True)  # tar bort tom kolumn
        columns = [c.strip() for c in eon_cost.iloc[0, :].tolist()]  # trims whitespace
        eon_cost.columns = columns
        eon_cost.drop(labels=0, axis=0, inplace=True)
        eon_cost[["Nät", "Energi", "Energiskatt"]] = eon_cost[
            ["Nät", "Energi", "Energiskatt"]
        ].astype(float)
        eon_cost["Nät inkl moms"] = (eon_cost["Nät"] * 1.25).round(2)
        eon_cost["Energi inkl moms"] = (eon_cost["Energi"] * 1.25).round(2)
        eon_cost["Energiskatt inkl moms"] = (eon_cost["Energiskatt"] * 1.25).round(2)
        eon_cost["Summa inkl moms"] = (eon_cost["Summa"] * 1.25).astype(float).round(2)
        column_order = [
            "Anläggnings-ID",
            "Förbrukningsperiod",
            "Referens",
            "Status",
            "Nät inkl moms",
            "Energi inkl moms",
            "Energiskatt inkl moms",
            "Summa inkl moms",
            "Nät",
            "Energi",
            "Energiskatt",
            "Varav Moms",
            "Summa",
        ]
        eon_cost = eon_cost[column_order]
        eon_cost = eon_cost.rename(
            columns={
                "Summa": "Summa exkl moms",
                "Varav Moms": "Moms",
                "Nät": "Nät exkl moms",
                "Energi": "Energi exkl moms",
                "Energiskatt": "Energiskatt exkl moms",
            }
        )
        eon_cost = pd.melt(
            eon_cost,
            id_vars=["Anläggnings-ID", "Förbrukningsperiod", "Referens", "Status"],
            var_name="Typ",
            value_name="Kostnad",
        )
        # print(eon_cost)
        eon_cost = eon_cost[pd.notnull(eon_cost["Kostnad"])]
        eon_cost = eon_cost.where(pd.notnull(eon_cost), None)
        data = [tuple(i) for i in eon_cost.values.tolist()]
        return data

    def meta(self):
        """downloads metadata from eon

        """
        try:
            driver = self.log_in_eon()
            # välj Energikarta
            driver.find_element_by_xpath(
                '//*[@id="body"]/div[2]/div/div[2]/ul/li[2]'
            ).click()
            ts(5)
            driver.find_element_by_xpath(
                '//*[@id="body"]/div[2]/div/div[2]/ul/li[2]/ul/li[1]/a'
            ).click()
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
            driver.find_element_by_id("test1").click()
            ts(5)
            driver.find_element_by_xpath(
                '//*[@id="installation-list-container"]/div[1]/div[1]/div[1]/div/button[1]'
            ).click()
            # waits for file to download
            Energi._check_folder(self._downloadPath)
            # ts(15)
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
        """transforms meta data

        """
        p = Path(self._downloadPath)
        file = list(p.glob("**/*.xlsx"))
        if len(file) > 1:
            raise IOError(f"There are more than one file in {self._downloadPath}")
        try:
            file = file[0]
        except IndexError:
            raise IOError(
                f"The folder {self._downloadPath} is empty. Is there an error when downloading the file?"
            )
        meta_eon = pd.read_excel(file, skiprows=9, converters={"Anläggnings-ID ": str})
        meta_eon.dropna(axis=1, how="all", inplace=True)
        # meta_eon = meta_eon[meta_eon['Mätmetod '] == 'Tim']
        column_order = [
            "Anläggnings-ID ",
            "Ort ",
            "Elområde ",
            "Nät-ID ",
            "Anläggningsadress ",
            "Företag ",
            "Organisationsnummer ",
            "Anläggningstyp ",
            "Aktiv ",
            "Mätmetod ",
            "Nätprislista ",
            "Nätägare ",
            "Säkringsnivå (A) ",
            "Centralbet. ",
            "Betald ansl.kap (kW) ",
        ]
        meta_eon = meta_eon[column_order]
        meta_eon = meta_eon.where(pd.notnull(meta_eon), None)
        data = [tuple(i) for i in meta_eon.values.tolist()]
        return data

    def los_cost_transform(self):
        """data transformation of los excel file. 
        
        """
        p = Path(self._downloadPath)
        _file = list(p.glob("**/*.xls*"))
        if len(_file) > 1:
            raise IOError(f"There are more than one file in {self._downloadPath}")
        try:
            _file = _file[0]
        except IndexError:
            raise IOError(
                f"The folder {self._downloadPath} is empty. Is there an error when downloading the file?"
            )
        df = pd.read_excel(_file, header=None)
        period = df.iloc[0, 0][7:13]
        period = period[:4] + "-" + period[4:]
        data = df.iloc[2:, :]
        data.insert(0, "Period", period)
        cols = [
            "Period",
            "Kundnamn",
            "Org_nr",
            "Anläggningsadress",
            "Anläggningsid",
            "Nätområde",
            "Elområde",
            "Nätägare",
            "Ediel-ID Nätägare",
            "Beräknad årsförbrukning",
            "Mätmetod",
            "Avräkningsmetod",
            "Anläggningstyp",
            "Avläsningsdatum1",
            "Mätarställning1",
            "Avläsningsdatum2",
            "Mätarställning2",
            "LOS Energy-nummer",
            "Startdatum",
            "Slutdatum",
            "Frifält 1 Objekt",
            "Frifält 2 Referens",
            "Frifält 3 Kontosträng",
            "Kundnummer",
            "Månadsförbr (kWh)",
            "Månadsförbr avser",
            "Volym kWh",
            "Nät",
            "Arvode nät",
            "Energi kr",
            "Handelsavgifter (kr)",
            "Prissäkring (kr)",
            "Arvode (kr)",
            "Effektreserv (kr)",
            "Miljöel (kr)",
            "Elcertifikat (kr)",
            "Elcertifikat arvode (kr)",
            "Övrigt (kr)",
            "Elskatt (kr)",
            "Summa (kr)",
            "Moms (kr)",
        ]
        data.columns = cols
        data.reset_index(drop=True, inplace=True)
        data = data.copy()
        if data["Summa (kr)"].sum() == 0:
            return
        else:
            data["Avläsningsdatum1"] = pd.to_datetime(
                data["Avläsningsdatum1"], yearfirst=True
            ).astype(str)
            data["Avläsningsdatum2"] = pd.to_datetime(
                data["Avläsningsdatum2"], yearfirst=True
            ).astype(str)
            data["Startdatum"] = pd.to_datetime(
                data["Startdatum"], yearfirst=True
            ).astype(str)
            data["Slutdatum"] = data["Slutdatum"].apply(lambda x: (str(x)[:10]))
            data["Kundnummer"] = data["Kundnummer"].astype(
                str
            )  # must be set as a string, else pymssql tries to insert it as int
            data["LOS Energy-nummer"] = data["LOS Energy-nummer"].astype(str)
            data["Org_nr"] = data["Org_nr"].astype(str)
            data["Ediel-ID Nätägare"] = data["Ediel-ID Nätägare"].astype(str)
            data_cl = data.where(pd.notnull(data), None)
            data = [tuple(i) for i in data_cl.values.tolist()]
            return data

    def db_insert(
        self,
        data: List[Tuple[Any, ...]],
        server: str,
        db: str,
        table: str,
        user: str,
        pw: str,
        truncate: bool = False,
    ) -> None:
        """inserts data into database 
        
        :param data: data to insert into database.
        :type data: List[Tuple[Any, ...]]
        :param server: Name of server
        :type server: str
        :param db: Name of database
        :type db: str
        :param table: Name of table
        :type table: str
        :param user: name of user to login to database
        :type user: str
        :param pw: Password to db for user
        :type pw: str
        :param truncate: if table in database should be truncated, optional, defaults to False
        :type truncate: bool
        :param controlForDuplicates: if True the fucntion controles for duplicates in table, optional, defaults to True
        :type data: bool
        """
        if not data:
            return
        _truncate: str
        if truncate:
            _truncate = "yes"
        else:
            _truncate = "no"
        mssql_insert(
            server=server,
            db=db,
            table=table,
            data=data,
            user=user,
            password=pw,
            truncate=_truncate,
        )

    @staticmethod
    def db_delete_records(
        server: str,
        user: str,
        database: str,
        password: str,
        table: str,
        whereClause: str,
    ) -> None:
        """
        """
        statement = f"""DELETE FROM {table} WHERE {whereClause}"""
        mssql_insert(
            statement=statement,
            server=server,
            db=database,
            user=user,
            password=password,
            table=table,
        )

    def clean_folder(self, destinationFolder: str = None) -> None:
        """Deletes all files in given dir or moves them to another dir.
        To move to another dir ther parameter destinationFolder must be specified.
        
        :param destinationFolder: folder to put data. If None the file will be erased, defaults to None.
        :type destinationFolder: str
        """
        p: Path
        path_content: List[Path]
        d: str = destinationFolder
        destination_path: Path
        p = Path(self._downloadPath)
        path_content = list(p.glob("**/*.xls*"))
        if path_content:
            for f in path_content:
                if not d:
                    os.remove(f)
                else:
                    path = f
                    destination_path = path.parent.parent / d / path.name
                    path.rename(destination_path)

    @staticmethod
    def _check_folder(folder: str, minutes: int = 2) -> None:
        """
        controls if folder is empty. Raises error if time exceeds int given
        in minutes. default 2
        """
        _folder: Path = Path(folder)
        timeout = time.time() + 60 * minutes  # 2 minutes
        while True:
            if time.time() > timeout:
                break  #
            path_content = list(_folder.glob("**/*.xls*"))
            if not path_content:
                ts(5)
                continue
            else:
                # print(time.time() - (timeout - 120))
                break

    def _control_for_duplicates(
        self,
        data: List[Tuple[Any, ...]],
        server: str,
        db: str,
        table: str,
        user: str,
        password: str,
    ) -> Optional[str]:
        """
        controls table if data already exists
        """
        if self._datasource == "eon_consumption":
            query = f"SELECT * FROM {table} WHERE YEAR([Timestamp])={self.year} AND MONTH([TIMESTAMP])={self.month}"
        elif self._datasource == "eon_cost":
            query = None
        elif self._datasource == "eon_meta":
            query = None
        elif self._datasource == "los":
            if self.month < 10:
                m = str("0" + str(self.month))
                period = str(self.year) + "-" + m
            else:
                period = str(self.year) + "-" + str(self.month)
            query = f"SELECT * FROM {table} WHERE [Period]='{period}';"
        else:
            query = None
        return query

    def _rename_file(self, facility: str):
        """helper method that renames given file
        """
        folder: Path = Path(self._downloadPath)
        files: List = list(folder.glob("*.csv"))
        latest_file: str = max(files, key=os.path.getctime)
        name: str = facility + ".csv"
        Path(latest_file).replace(folder / name)
        print(latest_file)

    def _createTertial(self):
        """helper method to create month in given quarter 
        """
        startYear = str(self.year)
        if self._month == 3:
            startMonth = self.month + 9
            middleMonth1 = self.month - 2
            middleMonth2 = self.month - 1
            startYear = str(self.year - 1)
        elif self._month == 2:
            startMonth = self._month + 9
            middleMonth1 = self.month + 10
            middleMonth2 = self.month - 1
            startYear = str(self.year - 1)
        elif self._month == 1:
            startMonth = self._month + 9
            middleMonth1 = self.month + 10
            middleMonth2 = self.month + 11
            startYear = str(self.year - 1)
        else:
            startMonth = self.month - 3
            middleMonth1 = self.month - 2
            middleMonth2 = self.month - 1
        startMonth = str(startMonth)
        middleMonth1 = str(middleMonth1)
        middleMonth2 = str(middleMonth2)
        endMonth = str(self.month)
        if int(startMonth) < 10:
            startMonth = "0" + startMonth
        if int(middleMonth1) < 10:
            middleMonth1 = "0" + middleMonth1
        if int(middleMonth2) < 10:
            middleMonth2 = "0" + middleMonth2
        if int(endMonth) < 10:
            endMonth = "0" + endMonth
        endYear = str(self._year)
        _months = [startMonth, middleMonth1, middleMonth2, endMonth]
        if startYear == endYear:
            _quarter = [endYear + "-" + m for m in _months]
        else:
            if self._month == 3:
                _quarter = [
                    startYear + "-" + startMonth,
                    endYear + "-" + middleMonth1,
                    endYear + "-" + middleMonth2,
                    endYear + "-" + endMonth,
                ]
            elif self._month == 2:
                _quarter = [
                    startYear + "-" + startMonth,
                    startYear + "-" + middleMonth1,
                    endYear + "-" + middleMonth2,
                    endYear + "-" + endMonth,
                ]
            elif self._month == 1:
                _quarter = [
                    startYear + "-" + startMonth,
                    startYear + "-" + middleMonth1,
                    startYear + "-" + middleMonth2,
                    endYear + "-" + endMonth,
                ]
            else:
                _quarter = [
                    endYear + "-" + startMonth,
                    endYear + "-" + middleMonth1,
                    endYear + "-" + middleMonth2,
                    endYear + "-" + endMonth,
                ]
        return _quarter

    @property
    def year(self):
        return self._year

    @property
    def month(self):
        return self._month

    @property
    def period(self):
        m = str(self._month)
        if len(m) == 1:
            m = "0" + m
        return str(self._year) + m

    @property
    def period2(self):
        m = str(self.month)
        if len(m) == 1:
            m = "0" + m
        return str(self.year) + "-" + m

    @property
    def tertial(self):
        return self._createTertial()

    @property
    def downloadPath(self):
        return self._downloadPath
