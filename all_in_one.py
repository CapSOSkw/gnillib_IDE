'''
**
* @Author: Keyuan Wu
* @Update: 04/26/2018
* :Python 3.6.5
**
'''
import pandas as pd
import numpy as np
import geocoder
import random
from time import sleep
import re
from datetime import datetime, timedelta
import sys
import os, math
import requests
from PyQt5.QtCore import QCoreApplication, Qt
from PyQt5.QtWidgets import QApplication, QTableWidgetItem, QTableWidget, QWidget, QMainWindow, QPushButton, QAction, QMessageBox, QVBoxLayout, QTabWidget, QFormLayout
from PyQt5.QtWidgets import QCalendarWidget, QFontDialog, QColorDialog, QTextEdit, QFileDialog, QComboBox
from PyQt5.QtWidgets import QCheckBox, QProgressBar, QComboBox, QLabel, QStyleFactory, QLineEdit, QInputDialog, QHBoxLayout, QGridLayout
from PyQt5.QtCore import pyqtSlot
import sqlite3
from sqlalchemy import create_engine
from pprint import pprint


class info_locker(object):
    nyc_zip = [10001, 10002, 10003, 10004, 10005, 10006, 10007, 10008, 10009,
               10010, 10011, 10012, 10013, 10014, 10015, 10016, 10017, 10018, 10019, 10020, 10021, 10022, 10023, 10024,
               10025, 10026,
               10027, 10028, 10029, 10030, 10031, 10032, 10033, 10034, 10035, 10036, 10037, 10038, 10039, 10040, 10041,
               10043, 10044,
               10045, 10046, 10047, 10048, 10055, 10060, 10065, 10069, 10072, 10075, 10079, 10080, 10081, 10082, 10087,
               10090, 10094,
               10095, 10096, 10098, 10099, 10101, 10102, 10103, 10104, 10105, 10106, 10107, 10108, 10109, 10110, 10111,
               10112, 10113, 10114,
               10115, 10116, 10117, 10118, 10119, 10120, 10121, 10122, 10123, 10124, 10125, 10126, 10128, 10129, 10130,
               10131, 10132,
               10133, 10138, 10149, 10150, 10151, 10152, 10153, 10154, 10155, 10156, 10157, 10158, 10159, 10160, 10161,
               10162, 10163,
               10164, 10165, 10166, 10167, 10168, 10169, 10170, 10171, 10172, 10173, 10174, 10175, 10176, 10177, 10178,
               10179, 10184,
               10185, 10196, 10197, 10199, 10203, 10211, 10212, 10213, 10242, 10249, 10256, 10257, 10258, 10259, 10260,
               10261, 10265,
               10268, 10269, 10270, 10271, 10272, 10273, 10274, 10275, 10276, 10277, 10278, 10279, 10280, 10281, 10282,
               10285, 10286,
               10292, 10301, 10302, 10303, 10304, 10305, 10306, 10307, 10308, 10309, 10310, 10311, 10312, 10313, 10314,
               10451, 10452,
               10453, 10454, 10455, 10456, 10457, 10458, 10459, 10460, 10461, 10462, 10463, 10464, 10465, 10466, 10467,
               10468, 10469,
               10470, 10471, 10472, 10473, 10474, 10475, 10499, 11101, 11102, 11103, 11104, 11105, 11106, 11109, 11120,
               11201, 11202,
               11203, 11204, 11205, 11206, 11207, 11208, 11209, 11210, 11211, 11212, 11213, 11214, 11215, 11216, 11217,
               11218, 11219,
               11220, 11221, 11222, 11223, 11224, 11225, 11226, 11228, 11229, 11230, 11231, 11232, 11233, 11234, 11235,
               11236, 11237,
               11238, 11239, 11240, 11241, 11242, 11243, 11240, 11245, 11247, 11248, 11249, 11251, 11252, 11354, 11255,
               11256, 11351,
               11352, 11354, 11355, 11356, 11357, 11358, 11359, 11360, 11361, 11362, 11363, 11364, 11365, 11366, 11367,
               11368, 11369,
               11370, 11371, 11372, 11373, 11374, 11375, 11377, 11378, 11379, 11380, 11381, 11385, 11386, 11390, 11405,
               11411, 11412,
               11413, 11414, 11415, 11416, 11417, 11418, 11419, 11420, 11421, 11422, 11423, 11424, 11425, 11426, 11427,
               11428, 11429,
               11430, 11431, 11432, 11433, 11434, 11435, 11436, 11439, 11451, 11499, 11690, 11691, 11692, 11693, 11694,
               11695, 11697, ]

    under_110st_zip = [10001, 10002, 10003, 10004, 10005, 10006, 10007, 10008, 10009, 10010,
                       10011, 10012, 10013, 10014, 10015, 10016, 10017, 10018, 10019, 10020,
                       10021, 10022, 10023, 10024, 10028, 10036, 10038, 10041, 10043, 10044,
                       10045, 10048, 10055, 10060, 10065, 10069, 10075, 10080, 10081, 10087,
                       10090, 10095, 10101, 10102, 10103, 10104, 10105, 10106, 10107, 10108,
                       10109, 10110, 10111, 10112, 10113, 10114, 10116, 10117, 10118, 10119,
                       10120, 10121, 10122, 10123, 10124, 10125, 10126, 10128, 10129, 10130,
                       10131, 10132, 10133, 10138, 10150, 10151, 10152, 10153, 10154, 10155,
                       10156, 10157, 10158, 10159, 10160, 10161, 10162, 10163, 10164, 10165,
                       10166, 10167, 10168, 10169, 10170, 10171, 10172, 10173, 10174, 10175,
                       10176, 10177, 10178, 10179, 10185, 10199, 10203, 10211, 10212, 10213,
                       10242, 10249, 10256, 10257, 10258, 10259, 10260, 10261, 10265, 10268,
                       10269, 10270, 10271, 10272, 10273, 10274, 10275, 10276, 10277, 10278,
                       10279, 10280, 10281, 10282, 10285, 10286, 10292, 10029, 10025]

    zip_across_110 = [10025, 10026, 10029]

    queens_but_not_nass_zip = [11001, 11004, 11005, 11040]

    NYSDOH = {
        'name': 'NYSDOH',
        'ETIN': 'EMEDNYBAT',
        'id': '141797357',
        'address': 'CORNING TOWER EMPIRE STATE PLAZA',
        'city': 'ALBANY',
        'state': 'NY',
        'zipcode': '12237',

    }

    version_code = {
        '837': '005010X222A1',
        '276': '005010X212',
        '270': '005010X279A1'
    }

    decoding_info = {
        '0': {'code': 'A0100', 'modifier': "", 'price': 25.95},
        '1': {'code': 'A0100', 'modifier': "TN", 'price': 35},
        '2': {'code': 'S0215', 'modifier': "", 'price': 3.21},
        '3': {'code': 'S0215', 'modifier': "TN", 'price': 2.25},
        '4': {'code': 'A0100', 'modifier': "SC", 'price': 25},
        '5': {'code': 'A0170', 'modifier': "CG", }
    }

    base_info = None

    driver_information = None


class EDI270():

    def __init__(self, file):

        if isinstance(file, pd.DataFrame):     #if input is pandas dataframe type, use it directly or read from csv or excel
            self.df = file
        else:
            self.df = pd.read_csv(file, dtype=object) if file[-1] == 'v' else pd.read_excel(file, dtype=object)

        try:
            self.df['SVC DATE'] = self.df['SVC DATE'].apply(lambda x: datetime.strptime(str(x), "%Y-%m-%d %H:%M:%S").strftime("%Y%m%d"))
        except:
            self.df['SVC DATE'] = self.df['SVC DATE'].apply(
                lambda x: datetime.strptime(str(x), "%m/%d/%Y").strftime("%Y%m%d"))

        try:
            self.df['DOB'] = self.df['DOB'].apply(lambda x: datetime.strptime(str(x), "%Y-%m-%d %H:%M:%S").strftime("%Y%m%d"))
        except:
            self.df['DOB'] = self.df['DOB'].apply(
                lambda x: datetime.strptime(str(x), "%m/%d/%Y").strftime("%Y%m%d"))

        self.transaction_num = self.df.__len__()   # the number of trips in data
        self.st_se_fixed_lines = 13   # fixed number of lines between ST and SE(each section)

        self.date_format1 = datetime.today().date().strftime("%y%m%d")  # used in ISA
        self.date_format2 = datetime.today().date().strftime("%Y%m%d")
        self.time_format = datetime.now().time().strftime("%H%M%S")
        self.interchange_ctrl_number = "0" + str(self.date_format1) + str(self.time_format[2:4])

        self.version_code = info_locker.version_code['270']
        self.submitter_info = info_locker.clean_air_base
        self.receiver_info = info_locker.NYSDOH

        self.file_name = '270-' + self.date_format2 + self.time_format + ".txt"

    def ISA(self, prod=True):
        if prod==False:    # PTE mode

            ISA = ["ISA", "00", " "*10, "00", " "*10, "ZZ", '{:<15s}'.format(self.submitter_info['ETIN']),
                    "ZZ", '{:<15s}'.format(self.receiver_info['ETIN']), str(self.date_format1),
                    str(self.time_format[:-2]), '^', '00501', self.interchange_ctrl_number, '0', 'T', ':~']

        else:     # production mode

            ISA = ["ISA", "00", " "*10, "00", " "*10, "ZZ", '{:<15s}'.format(self.submitter_info['ETIN']),
                    "ZZ", '{:<15s}'.format(self.receiver_info['ETIN']), str(self.date_format1),
                    str(self.time_format[:-2]), '^', '00501', self.interchange_ctrl_number, '1', 'P', ':~']

        return '*'.join(ISA)

    def GS(self):
        GS = ["GS", "HS", self.submitter_info['ETIN'], self.receiver_info['ETIN'], self.date_format2,
              self.time_format[:-2], "1", "X", self.version_code]

        return '*'.join(GS) + "~"

    def transaction_header(self, iterations, invoice_number):
        ST = ["ST", "270", str('{:>04d}'.format(iterations)), self.version_code]
        BHT = ["BHT", "0022", "13", str(invoice_number).strip(), self.date_format2, self.time_format[:-2]]

        return '*'.join(ST) + "~" + '*'.join(BHT) + "~"

    def first_HL(self):
        HL = ["HL", "1", "", "20", "1"]
        NM1 = ["NM1", "PR", "2", self.receiver_info['name'], "", "", "", "", "FI", self.receiver_info['id']]

        return "*".join(HL) + "~" + "*".join(NM1) + "~"

    def second_HL(self, service_name, service_npi):
        HL = ["HL", "2", "1", "21", "1"]
        NM1 = ["NM1", "1P", "2", service_name.strip().replace(",", ""), "", "", "", "", "XX", str(service_npi).strip()]
        REF = ["REF", "EO", self.submitter_info['MedicaidProviderNum']]

        return "*".join(HL) + "~" + "*".join(NM1) + "~" + "*".join(REF) + "~"

    def third_HL(self, patient_lastname, patient_firstname, medicaid_number, dob, gender, service_date):
        HL = ["HL", "3", "2", "22", "0"]
        NM1 = ["NM1", "IL", "1", patient_lastname.strip().upper(), patient_firstname.strip().upper(), "", "", "", "MI", medicaid_number.strip().upper()]
        DMG = ["DMG", "D8", str(dob), gender.upper()]
        DTP = ["DTP", "291", "D8", str(service_date)]
        EQ = ["EQ", "30"]

        return "*".join(HL) + "~" + "*".join(NM1) + "~" + "*".join(DMG) + "~" + "*".join(DTP) + "~" + "*".join(EQ) + "~"

    def transaction_trailer(self, iterations):
        SE = ["SE", "13", str('{:>04d}'.format(iterations))]
        return '*'.join(SE) + "~"

    def GE(self, count_ST):
        GE = ["GE", str(count_ST), "1"]

        return '*'.join(GE) + "~"

    def IEA(self):
        IEA = ["IEA", "1", self.interchange_ctrl_number]

        return "*".join(IEA) + "~"

    def ST_SE_loop(self):

        result = []
        for row in range(self.transaction_num):
            df_row = self.df.ix[[row]]
            ST = self.transaction_header(iterations=row+1, invoice_number=df_row['INVOICE NUMBER'].values[0])
            first_HL = self.first_HL()
            second_HL = self.second_HL(service_name=df_row['SVC NAME'].values[0], service_npi=df_row['SVC NPI'].values[0])
            third_HL = self.third_HL(patient_lastname=df_row['CLIENT LAST NAME'].values[0], patient_firstname=df_row['CLIENT FIRST NAME'].values[0],
                                     medicaid_number=df_row['MEDICAID ID NUMBER'].values[0], dob=df_row['DOB'].values[0],
                                     gender=df_row['GENDER'].values[0], service_date=df_row['SVC DATE'].values[0])
            SE = self.transaction_trailer(iterations=row+1)

            merged_loop = ST + first_HL + second_HL + third_HL + SE
            result.append(merged_loop)

        return "".join(result)

    def ISA_IEA(self):
        ISA = self.ISA()
        GS = self.GS()
        ST_SE = self.ST_SE_loop()
        GE = self.GE(self.transaction_num)
        IEA = self.IEA()

        return ISA + GS + ST_SE + GE + IEA


class EDI276():

    def __init__(self, file):
        if isinstance(file, pd.DataFrame):
            self.df = file
        else:
            self.df = pd.read_csv(file) if file[-1] == 'v' else pd.read_excel(file)

        # print(self.df)
        # try:
        #     self.df['SVC DATE'] = self.df['SVC DATE'].apply(lambda x: datetime.strptime(str(x), "%Y%m%d").strftime("%Y%m%d"))
        # except:
        #     self.df['SVC DATE'] = self.df['SVC DATE'].apply(
        #         lambda x: datetime.strptime(str(x), "%m/%d/%Y").strftime("%Y%m%d"))
        #
        # try:
        #     self.df['DOB'] = self.df['DOB'].apply(lambda x: datetime.strptime(str(x), "%Y%m%d").strftime("%Y%m%d"))
        # except:
        #     self.df['DOB'] = self.df['DOB'].apply(
        #         lambda x: datetime.strptime(str(x), "%m/%d/%Y").strftime("%Y%m%d"))

        self.transaction_num = self.df.__len__()
        self.st_se_fixed_lines = 15

        self.date_format1 = datetime.today().date().strftime("%y%m%d")  # used in ISA
        self.date_format2 = datetime.today().date().strftime("%Y%m%d")
        self.time_format = datetime.now().time().strftime("%H%M")
        self.interchange_ctrl_number = "0" + str(self.date_format1) + str(self.time_format[2:])

        self.version_code = info_locker.version_code['276']
        self.submitter_info = info_locker.clean_air_base
        self.bill_provider = info_locker.base_info
        self.receiver_info = info_locker.NYSDOH

        self.file_name = '276-' + self.date_format2 + self.time_format + ".txt"

    def ISA(self, prod=True):
        if prod==False:

            ISA = ["ISA", "00", " "*10, "00", " "*10, "ZZ", '{:<15s}'.format(self.submitter_info['ETIN']),
                    "ZZ", '{:<15s}'.format(self.receiver_info['ETIN']), str(self.date_format1),
                    str(self.time_format), '^', '00501', self.interchange_ctrl_number, '0', 'T', ':~']

        else:

            ISA = ["ISA", "00", " "*10, "00", " "*10, "ZZ", '{:<15s}'.format(self.submitter_info['ETIN']),
                    "ZZ", '{:<15s}'.format(self.receiver_info['ETIN']), str(self.date_format1),
                    str(self.time_format), '^', '00501', self.interchange_ctrl_number, '1', 'P', ':~']

        return '*'.join(ISA)

    def GS(self):
        GS = ["GS", "HR", self.submitter_info['ETIN'], self.receiver_info['ETIN'], self.date_format2,
              self.time_format, "1", "X", self.version_code]

        return '*'.join(GS) + "~"

    def transaction_header(self, iterations, invoice_number):
        ST = ["ST", "276", str('{:>04d}'.format(iterations)), self.version_code]
        BHT = ["BHT", "0010", "13", str(invoice_number).strip(), self.date_format2, self.time_format]

        return '*'.join(ST) + "~" + '*'.join(BHT) + "~"

    def first_HL(self): # payer info
        HL = ["HL", "1", "", "20", "1"]
        NM1 = ["NM1", "PR", "2", self.receiver_info['name'], "", "", "", "", "PI", self.receiver_info['id']]

        return "*".join(HL) + "~" + "*".join(NM1) + "~"

    def second_HL(self): # submitter info
        HL = ["HL", "2", "1", "21", "1"]
        NM1 = ['NM1', '41', '2', self.submitter_info['BaseName'], '', '', '', '', '46', self.submitter_info['ETIN']]

        return "*".join(HL) + "~" + "*".join(NM1) + "~"

    def third_HL(self): # PROVIDER INFO  SV: medicaid #, XX: NPI
        HL = ['HL', '3', '2', '19', '1']
        # print(self.bill_provider['MedicaidProviderNum'])
        NM1 = ['NM1', '1P', '2', self.bill_provider['BaseName'], '', '', '', '', 'SV', self.bill_provider['MedicaidProviderNum']]

        return "*".join(HL) + "~" + "*".join(NM1) + "~"

    def fourth_HL(self, dob, gender, patient_lastname, patient_firstname, medicaid_number, claim_ctl_number, service_date, invoice_number):
        HL = ['HL', '4', '3', '22', '0']
        DMG = ['DMG', 'D8', str(dob), gender.upper()]
        NM1 = ['NM1', 'IL', '1', patient_lastname.strip().upper(), patient_firstname.strip().upper(), '', '', '', 'MI', medicaid_number]
        TRN = ['TRN', '1', str(invoice_number).strip()]
        REF = ['REF', '1K', str(claim_ctl_number)]
        DTP = ['DTP', '472', 'D8', service_date.__str__()]

        return "*".join(HL) + '~' + "*".join(DMG) + "~" + "*".join(NM1) + "~" + "*".join(TRN) + "~" + "*".join(REF) + "~" + "*".join(DTP) + "~"

    def transaction_trailer(self, iterations):
        SE = ['SE', '15', str('{:>04d}'.format(iterations))]
        return '*'.join(SE) + "~"

    def GE(self, count_ST):
        GE = ['GE', count_ST.__str__(), '1']
        return '*'.join(GE) + '~'

    def IEA(self):
        IEA = ["IEA", "1", self.interchange_ctrl_number]

        return "*".join(IEA) + "~"

    def ST_SE_loop(self):

        result = []
        for row in range(self.transaction_num):
            df_row = self.df.ix[[row]]
            ST = self.transaction_header(iterations=row+1, invoice_number=df_row['INVOICE NUMBER'].values[0])
            first_HL = self.first_HL()
            second_HL = self.second_HL()
            third_HL = self.third_HL()
            fourth_HL = self.fourth_HL(dob=df_row['DOB'].values[0], gender=df_row['GENDER'].values[0],
                                       patient_lastname=df_row['CLIENT LAST NAME'].values[0], patient_firstname=df_row['CLIENT FIRST NAME'].values[0],
                                       medicaid_number=df_row['MEDICAID ID NUMBER'].values[0], claim_ctl_number=df_row['CLAIM CONTROL NUMBER'].values[0],
                                       service_date=df_row['SVC DATE'].values[0], invoice_number=df_row['INVOICE NUMBER'].values[0])
            SE = self.transaction_trailer(iterations=row+1)

            merged_loop = ST + first_HL + second_HL + third_HL + fourth_HL + SE
            result.append(merged_loop)

        return ''.join(result)

    def ISA_IEA(self):
        ISA = self.ISA()
        GS = self.GS()
        ST_SE = self.ST_SE_loop()
        GE = self.GE(self.transaction_num)
        IEA = self.IEA()

        return ISA + GS + ST_SE + GE + IEA


class EDI837P():

    def __init__(self, file):
        self.df = pd.read_csv(file, dtype=object) if file[-1] == 'v' else pd.read_excel(file, dtype=object)
        # self.df = self.df.fillna("")

        self.df['service date'] = self.df['service date'].apply(lambda x: datetime.strptime(str(x), "%m/%d/%Y").strftime("%Y%m%d"))
        self.df['patient dob'] = self.df['patient dob'].apply(lambda x: datetime.strptime(str(x), "%m/%d/%Y").strftime("%Y%m%d"))
        self.df['patient pregnant'] = self.df['patient pregnant'].apply(lambda x: x == "Y")

        self.transaction_num = self.df.__len__()
        self.basic_line = 33
        self.lx_lines = 0

        self.submitter_info = info_locker.clean_air_base
        self.bill_provider = info_locker.base_info
        self.driver_info = info_locker.driver_information
        self.receiver_info = info_locker.NYSDOH
        self.version_code = info_locker.version_code['837']

        self.date_format1 = datetime.today().date().strftime("%y%m%d")  #used in ISA
        self.date_format2 = datetime.today().date().strftime("%Y%m%d")
        self.time_format = datetime.now().time().strftime("%H%M")
        self.interchange_ctrl_number = "0" + str(self.date_format1) + str(self.time_format[2:])
        # self.file_name = self.interchange_ctrl_number + '.txt'
        self.all_invoice_number = []
        self.invoice_ST_SE_dict = {}
        self.file_name = '837-' + re.findall(r'\d{4}-\d{2}-\d{2}-to-\d{4}-\d{2}-\d{2}', file)[0]  if re.findall(r'\d{4}-\d{2}-\d{2}-to-\d{4}-\d{2}-\d{2}', file).__len__() != 0 else '837-' + str(datetime.today().date())

    def ISA(self, prod=True):
        if prod==False:

            ISA = ["ISA", "00", " "*10, "00", " "*10, "ZZ", '{:<15s}'.format(self.submitter_info['ETIN']),
                    "ZZ", '{:<15s}'.format(self.receiver_info['ETIN']), str(self.date_format1),
                    str(self.time_format), '^', '00501', self.interchange_ctrl_number, '0', 'T', ':~']

        else:

            ISA = ["ISA", "00", " "*10, "00", " "*10, "ZZ", '{:<15s}'.format(self.submitter_info['ETIN']),
                    "ZZ", '{:<15s}'.format(self.receiver_info['ETIN']), str(self.date_format1),
                    str(self.time_format), '^', '00501', self.interchange_ctrl_number, '1', 'P', ':~']

        return '*'.join(ISA)

    def GS(self):
        GS = ["GS", "HC", self.submitter_info['ETIN'], self.receiver_info['ETIN'], self.date_format2,
              self.time_format, "1", "X", self.version_code]

        return '*'.join(GS) + "~"

    def transaction_header(self, iterations, invoice_number):
        ST = ["ST", "837", str('{:>04d}'.format(iterations)), self.version_code]
        BHT = ["BHT", "0019", "00", str(invoice_number), self.date_format2, self.time_format, "CH"]
        self.all_invoice_number.append(invoice_number)

        return '*'.join(ST) + "~" + '*'.join(BHT) + "~"

    def loop1000a(self):
        NM1 = ["NM1", "41", "2", self.submitter_info['BaseName'], "", "", "", "", "46", self.submitter_info['ETIN']]
        PER = ["PER", "IC", self.submitter_info['ContactName'], "TE", self.submitter_info['ContactTel'] ]

        return '*'.join(NM1) + "~" + '*'.join(PER) + "~"

    def loop1000b(self):
        NM1 = ["NM1", "40", "2", self.receiver_info['name'], "", "", "", "", "46", self.receiver_info['id']]

        return '*'.join(NM1) + "~"

    def loop2000a(self):   # billing provider HL
        HL = ["HL", "1", "", "20", "1"]

        return '*'.join(HL) + "~"

    def loop2010aa(self): #Billing provider info
        NM1 = ["NM1", "85", "2", self.bill_provider['BaseName']]
        N3 = ["N3", self.bill_provider['BaseAddress']]
        N4 = ["N4", self.bill_provider['City'], self.bill_provider['State'], self.bill_provider['zipcode']]
        REF = ["REF", "EI", self.bill_provider['TaxID']]

        return '*'.join(NM1) + "~" + '*'.join(N3) + "~" + '*'.join(N4) + "~" + '*'.join(REF) + "~"

    def loop2000b(self):  # subscriber HL
        HL = ["HL", "2", "1", "22", "0"]
        SBR = ["SBR", "P", "18", "", "", "", "", "", "", "MC"]

        return '*'.join(HL) + "~" + '*'.join(SBR) + "~"

    def loop2010ba(self, first, last, medi_num, address, city, state, zipcode, dob, gender):
        if first == "":
            first = "NoRecord"
        if last == "":
            last = "NoRecord"
        if medi_num == "":
            medi_num = "NoRecord"
        if address == "":
            address = 'NoRecord'
        if city == "":
            city = "NoRecord"
        if state == "":
            state = "NoRecord"
        if zipcode == "":
            zipcode = "00000"
        if dob == "":
            dob = "19000101"
        if gender == "":
            gender = "M"

        NM1 = ["NM1", "IL", "1", last.upper(), first.upper(), "", "", "", "MI", medi_num]
        N3 = ["N3", address.upper()]
        N4 = ["N4", city.upper(), state.upper(), str(zipcode)]
        DMG = ["DMG", "D8", str(dob), gender]

        return '*'.join(NM1) + "~" + '*'.join(N3) + "~" + '*'.join(N4) + "~" + '*'.join(DMG) + "~" # subscriber name

    def loop2010bb(self): # payer info
        NM1 = ["NM1", "PR", "2", self.receiver_info['name'], "", "", "", "", "PI", self.receiver_info['id']]
        N3 = ["N3", self.receiver_info['address']]
        N4 = ["N4", self.receiver_info['city'], self.receiver_info['state'], self.receiver_info['zipcode']]
        REF1 = ["REF", "G2", self.submitter_info['MedicaidProviderNum']]
        REF2 = ["REF", "LU", "003"]

        return '*'.join(NM1) + "~" + '*'.join(N3) + "~" + '*'.join(N4) + "~" + '*'.join(REF1) + "~" + '*'.join(REF2) + "~"

    def loop2300(self, invoice_number, amount, pa_num): #claim info
        if amount == "":
            amount = "0"
        if pa_num == "":
            pa_num = 0
        CLM = ["CLM", str(invoice_number), str(amount), "", "", "99:B:1", "Y", "A", "Y", "Y", "P"]
        REF = ['REF', "G1", str('{:>011d}'.format(int(pa_num)))]
        HI = ["HI", "ABK:R69"]

        return '*'.join(CLM) + "~" + '*'.join(REF) + "~" + '*'.join(HI) + "~"

    def loop2310a(self, driver_first, driver_last, driver_lic, service_name, service_NPI): #referring provider
        if driver_first == "":
            driver_first = "NoRecord"
        if driver_last == "":
            driver_last = "NoRecord"
        if driver_lic == "":
            driver_lic = 000000000
        if service_name == "":
            service_name = "NoRecord"
        if service_NPI == "":
            service_NPI = 000000000
        NM1 = ["NM1", "DN", "1", service_name.upper(), "", "", "", "", "XX", str(service_NPI)]
        NM1_1 = ["NM1", "P3", "1", driver_last.upper(), driver_first.upper()]
        REF = ["REF", "0B", str(driver_lic)]

        return '*'.join(NM1) + "~" + '*'.join(NM1_1) + "~" + '*'.join(REF) + "~"

    def loop2310b(self, driver_plate): #rendering provider name
        if driver_plate == "":
            driver_plate = 'A000000A'
        NM1 = ["NM1", "82", "2", self.submitter_info['BaseName']]
        REF = ["REF", "G2", driver_plate.upper()]

        return '*'.join(NM1) + "~" + "*".join(REF) + "~"

    def loop2310c(self, service_name, service_NPI, service_address, service_city, service_state, service_zip):
        NM1 = ["NM1", "77", "2", service_name.upper().replace(",", ""), "", "", "", "", "XX", str(service_NPI)]
        N3 = ["N3", service_address.upper()]
        N4 = ["N4", service_city.upper(), service_state.upper(), str(service_zip)]

        return '*'.join(NM1) + "~" + '*'.join(N3) + "~" + '*'.join(N4) + "~"

    def lx1(self, code, modifier, amount, unit, service_date):
        if str(modifier) == 'nan':
            SV1_01 = "HC:{0}".format(code)

        else:
            SV1_01 = "HC:{0}:{1}".format(code, modifier)

        if isinstance(unit, np.int64) == True:
            SV1_04 = str(int(unit))
        else:
            SV1_04 = str(unit)

        LX1 = ["LX", "1"]
        SV1 = ["SV1", SV1_01, str(amount), "UN", SV1_04, "", "", "1", "", "", "", "", "", "", "", "0"]
        DTP = ["DTP", "472", "D8", service_date]

        return '*'.join(LX1) + "~" + '*'.join(SV1) + "~" + '*'.join(DTP) + "~"

    def lx2(self, code, modifier, amount, unit, service_date):
        if str(modifier) == 'nan':
            SV2_01 = "HC:{0}".format(code)

        else:
            SV2_01 = "HC:{0}:{1}".format(code, modifier)

        if isinstance(unit, np.int64) == True:
            SV2_04 = str(int(unit))
        else:
            SV2_04 = str(unit)

        LX2 = ["LX", "2"]
        SV2 = ["SV1", SV2_01, str(amount), "UN", SV2_04, "", "", "1", "", "", "", "", "", "", "", "0"]
        DTP = ["DTP", "472", "D8", service_date]

        return '*'.join(LX2) + "~" + '*'.join(SV2) + "~" + '*'.join(DTP) + "~"

    def lx3(self, code, modifier, amount, unit, service_date):
        if str(modifier) == 'nan':
            SV3_01 = "HC:{0}".format(code)

        else:
            SV3_01 = "HC:{0}:{1}".format(code, modifier)


        if isinstance(unit, np.int64) == True:
            SV3_04 = str(int(unit))
        else:
            SV3_04 = str(unit)

        LX3 = ["LX", "3"]
        SV3 = ["SV1", SV3_01, str(amount), "UN", SV3_04 , "", "", "1", "", "", "", "", "", "", "", "0"]
        DTP = ["DTP", "472", "D8", service_date]

        return '*'.join(LX3) + "~" + '*'.join(SV3) + "~" + '*'.join(DTP) + "~"

    def lx4(self, code, modifier, amount, unit, service_date):
        if str(modifier) == 'nan':
            SV4_01 = "HC:{0}".format(code)

        else:
            SV4_01 = "HC:{0}:{1}".format(code, modifier)

        if isinstance(unit, np.int64) == True:
            SV4_04 = str(int(unit))
        else:
            SV4_04 = str(unit)

        LX4 = ["LX", "4"]
        SV4 = ["SV1", SV4_01, str(amount), "UN", SV4_04, "", "", "1", "", "", "", "", "", "", "", "0"]
        DTP = ["DTP", "472", "D8", service_date]

        return '*'.join(LX4) + "~" + '*'.join(SV4) + "~" + '*'.join(DTP) + "~"

    def lx5(self, code, modifier, amount, unit, service_date):
        if str(modifier) == 'nan':
            SV5_01 = "HC:{0}".format(code)

        else:
            SV5_01 = "HC:{0}:{1}".format(code, modifier)

        if isinstance(unit, np.int64) == True:
            SV5_04 = str(int(unit))
        else:
            SV5_04 = str(unit)

        LX5 = ["LX", "5"]
        SV5 = ["SV1", SV5_01, str(amount), "UN", SV5_04, "", "", "1", "", "", "", "", "", "", "", "0"]
        DTP = ["DTP", "472", "D8", service_date]

        return '*'.join(LX5) + "~" + '*'.join(SV5) + "~" + '*'.join(DTP) + "~"

    def lx6(self, code, modifier, amount, unit, service_date):
        if str(modifier) == 'nan':
            SV6_01 = "HC:{0}".format(code)

        else:
            SV6_01 = "HC:{0}:{1}".format(code, modifier)

        if isinstance(unit, np.int64) == True:
            SV6_04 = str(int(unit))
        else:
            SV6_04 = str(unit)

        LX6 = ["LX", "6"]
        SV6 = ["SV1", SV6_01, str(amount), "UN", SV6_04, "", "", "1", "", "", "", "", "", "", "", "0"]
        DTP = ["DTP", "472", "D8", service_date]

        return '*'.join(LX6) + "~" + '*'.join(SV6) + "~" + '*'.join(DTP) + "~"

    def loop2400(self, row_data):

        lx1 = self.lx1(code=row_data['service code 1'].values[0], amount=row_data['amount 1'].values[0], unit=row_data['unit 1'].values[0],
                       service_date=row_data['service date'].values[0], modifier=row_data['modifier code 1'].values[0])

        if row_data['service code 2'].isnull().values[0] == True:
            lx2 = lx3 = lx4 = lx5 = lx6 = ""
        else:
            lx2 = self.lx2(code=row_data['service code 2'].values[0], amount=row_data['amount 2'].values[0], unit=row_data['unit 2'].values[0],
                           service_date=row_data['service date'].values[0], modifier=row_data['modifier code 2'].values[0])

            if row_data['service code 3'].isnull().values[0] == True:
                lx3 = lx4 = lx5 = lx6 = ""

            else:
                lx3 = self.lx3(code=row_data['service code 3'].values[0], amount=row_data['amount 3'].values[0], unit=row_data['unit 3'].values[0],
                               service_date=row_data['service date'].values[0], modifier=row_data['modifier code 3'].values[0])

                if row_data['service code 4'].isnull().values[0] == True:
                    lx4 = lx5 = lx6 = ""

                else:
                    lx4 = self.lx4(code=row_data['service code 4'].values[0], amount=row_data['amount 4'].values[0], unit=row_data['unit 4'].values[0],
                               service_date=row_data['service date'].values[0], modifier=row_data['modifier code 4'].values[0])

                    if row_data['service code 5'].isnull().values[0] == True:
                        lx5 = lx6 = ""

                    else:
                        lx5 = self.lx5(code=row_data['service code 5'].values[0], amount=row_data['amount 5'].values[0], unit=row_data['unit 5'].values[0],
                               service_date=row_data['service date'].values[0], modifier=row_data['modifier code 5'].values[0])

                        if row_data['service code 6'].isnull().values[0] == True:
                            lx6 = ""
                        else:
                            lx6 = self.lx6(code=row_data['service code 6'].values[0], amount=row_data['amount 6'].values[0], unit=row_data['unit 6'].values[0],
                               service_date=row_data['service date'].values[0], modifier=row_data['modifier code 6'].values[0])

        if len(lx1) > 0:
            self.lx_lines += 3

        if len(lx2) > 0:
            self.lx_lines += 3

        if len(lx3) > 0:
            self.lx_lines += 3

        if len(lx4) > 0:
            self.lx_lines += 3

        if len(lx5) > 0:
            self.lx_lines += 3

        if len(lx6) > 0:
            self.lx_lines += 3

        return lx1+lx2+lx3+lx4+lx5+lx6

    def transaction_trailer(self, count_line, iterations):
        SE = ["SE", str(count_line), str('{:>04d}'.format(iterations))]
        return '*'.join(SE) + "~"

    def GE(self, count_ST):
        GE = ["GE", str(count_ST), "1"]

        return '*'.join(GE) + "~"

    def IEA(self):
        IEA = ["IEA", "1", self.interchange_ctrl_number]

        return '*'.join(IEA) + "~"

    def ST_SE_loop(self):

        result = []
        temp_invoice_num = []
        temp_ST_SE = []
        temp_patient_fn = []
        temp_patient_ln = []
        temp_patient_medicaid_num = []
        temp_service_date = []
        temp_837_name = []

        for row in range(self.transaction_num):
            self.lx_lines = 0
            df_row = self.df.ix[[row]]   # get row data

            ST = self.transaction_header(iterations=row+1, invoice_number= df_row['invoice number'].values[0])
            loop1000a = self.loop1000a()
            loop1000b = self.loop1000b()
            loop2000a = self.loop2000a()
            loop2010aa = self.loop2010aa()
            loop2000b = self.loop2000b()
            loop2010ba = self.loop2010ba(first=df_row['patient first name'].values[0], last=df_row['patient last name'].values[0], medi_num=df_row['patient medicaid number'].values[0],
                                         address=df_row['patient address'].values[0], city=df_row['patient city'].values[0], state=df_row['patient state'].values[0],
                                         zipcode=df_row['patient zip code'].values[0], dob=df_row['patient dob'].values[0], gender=df_row['patient gender'].values[0])
            loop2010bb = self.loop2010bb()
            loop2300 = self.loop2300(invoice_number=df_row['invoice number'].values[0], amount=df_row['claim_amount'].values[0], pa_num=df_row['pa number'].values[0])
            loop2310a = self.loop2310a(driver_first=df_row['driver first name'].values[0], driver_last=df_row['driver last name'].values[0],
                                       driver_lic=df_row['driver license number'].values[0], service_name=df_row['service facility name'].values[0],
                                       service_NPI=df_row['service npi'].values[0])
            loop2310b = self.loop2310b(driver_plate=df_row['driver plate number'].values[0])
            loop2310c = self.loop2310c(service_name=df_row['service facility name'].values[0], service_NPI=df_row['service npi'].values[0],
                                       service_address=df_row['service address'].values[0], service_city=df_row['service city'].values[0],
                                       service_state=df_row['service state'].values[0], service_zip=df_row['service zip code'].values[0])
            loop2400 = self.loop2400(df_row)

            lines_st_se = self.basic_line + self.lx_lines
            SE = self.transaction_trailer(count_line=lines_st_se, iterations=row+1)

            merge_loop = ST + loop1000a + loop1000b + loop2000a + loop2010aa + loop2000b + loop2010ba + loop2010bb + loop2300 + loop2310a + loop2310b + loop2310c + loop2400 + SE

            # self.invoice_ST_SE_dict[str(df_row['invoice number'].values[0])] = {'ST_SE loop': merge_loop,
            #                                                                     'Patient FN': df_row['patient first name'].values[0],
            #                                                                     'Patient LN': df_row['patient last name'].values[0],
            #                                                                     'Patient medicaid number': df_row['patient medicaid number'].values[0],
            #                                                                     'Service date': df_row['service date'].values[0],
            #                                                                     '837 file name': self.file_name,
            #                                                                     }
            temp_837_name.append(self.file_name)
            temp_invoice_num.append(df_row['invoice number'].values[0])
            temp_patient_fn.append(df_row['patient first name'].values[0])
            temp_patient_ln.append(df_row['patient last name'].values[0])
            temp_patient_medicaid_num.append(df_row['patient medicaid number'].values[0])
            temp_ST_SE.append(merge_loop)
            temp_service_date.append(df_row['service date'].values[0])

            result.append(merge_loop)

        self.invoice_ST_SE_dict = {'837 file name': temp_837_name,
                                   'Invoice number': temp_invoice_num,
                                   'Patient FN': temp_patient_fn,
                                   'Patient LN': temp_patient_ln,
                                   'Patient medicaid number': temp_patient_medicaid_num,
                                   'Service date': temp_service_date,
                                   'ST_SE': temp_ST_SE}
        return "".join(result)

    def ISA_IEA(self):
        ISA = self.ISA()
        GS = self.GS()
        ST_SE = self.ST_SE_loop()
        GE = self.GE(self.transaction_num)
        IEA = self.IEA()

        return ISA + GS + ST_SE + GE + IEA


class Process_MAS():

    def __init__(self, raw_file):

        self.raw_file = raw_file

        if self.raw_file[-1] == "t":   # determine how to read data
            read_data = pd.read_table
        elif self.raw_file[-1] == "v":
            read_data = pd.read_csv
        else:
            read_data = pd.read_excel

        self.raw_df = read_data(raw_file)   # read data
        self.raw_df['Pick-up Address'] = self.raw_df['Pick-up Address'].apply(lambda x: Process_Method().clean_address(x))        # clean address for the raw data
        self.raw_df['Drop-off Address'] = self.raw_df['Drop-off Address'].apply(lambda x: Process_Method().clean_address(x))

    def add_AB_leg(self, tocsv=False):

        unique_invoice = self.raw_df["Invoice Number"].unique().tolist()   # get unique invoice numbers

        for invoice_num in unique_invoice:
            idx = self.raw_df.loc[(self.raw_df['Invoice Number'] == invoice_num) & (self.raw_df['Record Type'] == 'Leg')].index   # get each invoice number's index, only for legs
            num_legs = idx.__len__()
            leg_name = ["A", "B", "C", "D"]     # ready for ABCD legs

            leg_id = []
            sorted_idx = []
            if num_legs == 4:    # if there are 4 legs, assign ABCD to legs based on leg_id's value from smallest to largest
                for i in idx:
                    leg_id.append(self.raw_df.ix[i, 'Leg ID'])

                leg_id.sort()

                for l in leg_id:
                    sorted_idx.append(self.raw_df.loc[self.raw_df['Leg ID'] == l].index[0])

                for index, l in enumerate(sorted_idx):

                    self.raw_df.ix[l, 'Invoice Number'] = str(self.raw_df.ix[l, 'Invoice Number']) + leg_name[index]

            else:
                for index, i in enumerate(idx):
                    self.raw_df.ix[i , 'Invoice Number'] = str(self.raw_df.ix[i, 'Invoice Number']) + leg_name[index]

        if tocsv == True:
            self.raw_df.to_csv("MAS-1.csv", index=False)

        return self.raw_df

    def add_codes(self, read_from_outside=False, tofile=False, file_outside=None):
        temp_df = pd.DataFrame()

        if read_from_outside == True:
            cd_df = pd.read_csv(file_outside)

        else:
            cd_df = self.add_AB_leg()

        cd_df['Code3'] = ""
        cd_df['Code3 Modifier'] = ""

        leg_idx = cd_df.loc[cd_df['Record Type'] == 'Leg'].index
        leg_idx_len = leg_idx.__len__()

        for i in leg_idx:

            # Legs all in NYC
            if (cd_df.ix[i, 'Pick-up Zip'] in info_locker.nyc_zip) and (cd_df.ix[i, "Drop-off Zip"] in info_locker.nyc_zip):
                if cd_df.ix[i, "Leg Mileage"] < 5:
                    cd_df.ix[i, "Code1"] = "A0100"
                    cd_df.ix[i, "Code1 Modifier"] = ""

                    cd_df.ix[i, "Code2"] = ""
                    cd_df.ix[i, "Code2 Modifier"] = ""

                elif 5 <= cd_df.ix[i, "Leg Mileage"] <= 8:
                    cd_df.ix[i, "Code1"] = "A0100"
                    cd_df.ix[i, "Code1 Modifier"] = "TN"

                    cd_df.ix[i, "Code2"] = ""
                    cd_df.ix[i, "Code2 Modifier"] = ""

                elif cd_df.ix[i, "Leg Mileage"] > 8:
                    cd_df.ix[i, "Code1"] = "A0100"
                    cd_df.ix[i, "Code1 Modifier"] = "TN"

                    cd_df.ix[i, "Code2"] = "S0215"
                    cd_df.ix[i, "Code2 Modifier"] = ""

                # one leg in NYC and the other leg not in nyc
            if (cd_df.ix[i, "Pick-up Zip"] not in info_locker.nyc_zip) or (cd_df.ix[i, "Drop-off Zip"] not in info_locker.nyc_zip):
                cd_df.ix[i, "Code1"] = "A0100"
                cd_df.ix[i, "Code1 Modifier"] = "TN"

                cd_df.ix[i, "Code2"] = "S0215"
                cd_df.ix[i, "Code2 Modifier"] = "TN"

             # in queens not in nass, use google APIs
            if (cd_df.ix[i, "Pick-up Zip"] in info_locker.queens_but_not_nass_zip) or (cd_df.ix[i, "Drop-off Zip"] in info_locker.queens_but_not_nass_zip):
                queens_nass_pickup_address = cd_df.ix[i, "Pick-up Address"] + " " + cd_df.ix[i, "Pick-up City"] + " " + str(cd_df.ix[i, "Pick-up Zip"].astype(int))
                queens_nass_dropoff_address = cd_df.ix[i, "Drop-off Address"] + " " + cd_df.ix[i, "Drop-off City"] + " " + str(cd_df.ix[i, "Drop-off Zip"].astype(int))

                queens_nass_pickup_address = re.sub(' +', ' ', queens_nass_pickup_address)
                queens_nass_dropoff_address = re.sub(' +', ' ', queens_nass_dropoff_address)

                bool_pickup_is_nassau = Process_Method().is_in_nassau(queens_nass_pickup_address)
                bool_dropoff_is_nassau = Process_Method().is_in_nassau(queens_nass_dropoff_address)

                if (bool_pickup_is_nassau==True and bool_dropoff_is_nassau==False) or\
                        (bool_pickup_is_nassau==False and bool_dropoff_is_nassau==True) or \
                        (bool_pickup_is_nassau == True and bool_dropoff_is_nassau == True):
                    cd_df.ix[i, "Code1"] = "A0100"
                    cd_df.ix[i, "Code1 Modifier"] = "TN"

                    cd_df.ix[i, "Code2"] = "S0215"
                    cd_df.ix[i, "Code2 Modifier"] = "TN"

                if (bool_pickup_is_nassau==False and bool_dropoff_is_nassau==False):
                    if cd_df.ix[i, "Leg Mileage"] < 5:
                        cd_df.ix[i, "Code1"] = "A0100"
                        cd_df.ix[i, "Code1 Modifier"] = ""

                        cd_df.ix[i, "Code2"] = ""
                        cd_df.ix[i, "Code2 Modifier"] = ""

                    elif 5 <= cd_df.ix[i, "Leg Mileage"] <= 8:
                        cd_df.ix[i, "Code1"] = "A0100"
                        cd_df.ix[i, "Code1 Modifier"] = "TN"

                        cd_df.ix[i, "Code2"] = ""
                        cd_df.ix[i, "Code2 Modifier"] = ""

                    elif cd_df.ix[i, "Leg Mileage"] > 8:
                        cd_df.ix[i, "Code1"] = "A0100"
                        cd_df.ix[i, "Code1 Modifier"] = "TN"

                        cd_df.ix[i, "Code2"] = "S0215"
                        cd_df.ix[i, "Code2 Modifier"] = ""



                # one leg under 110st, the other not in 110st
            zzzzz = cd_df.ix[i, "Pick-up Zip"]
            zzz = cd_df.ix[i, "Drop-off Zip"]
            if ((cd_df.ix[i, "Pick-up Zip"] in info_locker.under_110st_zip) and (cd_df.ix[i, "Pick-up Zip"] not in info_locker.zip_across_110) and (cd_df.ix[i, "Drop-off Zip"] not in info_locker.under_110st_zip)) or \
                    ((cd_df.ix[i, "Drop-off Zip"] in info_locker.under_110st_zip) and (cd_df.ix[i, "Drop-off Zip"] not in info_locker.zip_across_110) and (cd_df.ix[i, "Pick-up Zip"] not in info_locker.under_110st_zip)):
                if cd_df.ix[i, "Leg Mileage"] >= 3:

                    if cd_df.ix[i, "Code2"].__len__() == 0:
                        cd_df.ix[i, "Code2"] = "A0100"
                        cd_df.ix[i, "Code2 Modifier"] = "SC"
                    else:
                        cd_df.ix[i, "Code3"] = "A0100"
                        cd_df.ix[i, "Code3 Modifier"] = "SC"

                        # the same zip code across 110st
            if ((cd_df.ix[i, "Pick-up Zip"] in info_locker.zip_across_110) and (cd_df.ix[i, "Drop-off Zip"] in info_locker.zip_across_110)) and \
                    (cd_df.ix[i, "Leg Mileage"] >= 3):

                pickup_address = re.sub(' +', " ", cd_df.ix[i, 'Pick-up Address']) + " " + cd_df.ix[i, 'Pick-up City']
                dropoff_address = re.sub(' +', " ", cd_df.ix[i, 'Drop-off Address']) + " " + cd_df.ix[i, 'Drop-off City']
                pick_lat, pick_lng = Process_Method().google2geo(pickup_address)
                drop_lat, drop_lng = Process_Method().google2geo(dropoff_address)
                is_pick_under_110 = Process_Method().is_under_110st(pick_lat, pick_lng)
                is_drop_under_110 = Process_Method().is_under_110st(drop_lat, drop_lng)
                if (is_pick_under_110 == True and is_drop_under_110 == False) or \
                        (is_pick_under_110 == False and is_drop_under_110 == True):

                    if cd_df.ix[i, "Code2"].__len__() == 0:
                        cd_df.ix[i, "Code2"] = "A0100"
                        cd_df.ix[i, "Code2 Modifier"] = "SC"

                    else:
                        cd_df.ix[i, "Code3"] = "A0100"
                        cd_df.ix[i, "Code3 Modifier"] = "SC"

            #use zip code to deal with 110st issue
            if (((cd_df.ix[i, "Pick-up Zip"] in info_locker.zip_across_110) and (cd_df.ix[i, "Drop-off Zip"] not in info_locker.zip_across_110)) or
                    ((cd_df.ix[i, "Pick-up Zip"] not in info_locker.zip_across_110) and (cd_df.ix[i, "Drop-off Zip"] in info_locker.zip_across_110))) and \
                    (cd_df.ix[i, "Leg Mileage"] >= 3):

                pickup_address = re.sub(' +', " ", cd_df.ix[i, 'Pick-up Address']) + " " + cd_df.ix[i, 'Pick-up City']
                dropoff_address = re.sub(' +', " ", cd_df.ix[i, 'Drop-off Address']) + " " + cd_df.ix[
                    i, 'Drop-off City']
                pick_lat, pick_lng = Process_Method().google2geo(pickup_address)
                drop_lat, drop_lng = Process_Method().google2geo(dropoff_address)
                is_pick_under_110 = Process_Method().is_under_110st(pick_lat, pick_lng)
                is_drop_under_110 = Process_Method().is_under_110st(drop_lat, drop_lng)

                if ((is_pick_under_110 == False) and (cd_df.ix[i, "Drop-off Zip"] in info_locker.under_110st_zip)) or \
                        ((is_pick_under_110 == False) and (cd_df.ix[i, "Drop-off Zip"] in info_locker.zip_across_110) and is_drop_under_110==True) or \
                        ((cd_df.ix[i, "Pick-up Zip"] in info_locker.under_110st_zip) and (cd_df.ix[i, "Pick-up Zip"] not in info_locker.zip_across_110) and (is_drop_under_110 == False)) or \
                        ((cd_df.ix[i, "Pick-up Zip"] in info_locker.zip_across_110) and (is_pick_under_110==True) and (is_drop_under_110 == False)) or \
                        ((cd_df.ix[i, "Pick-up Zip"] in info_locker.zip_across_110) and (is_pick_under_110 == True) and (cd_df.ix[i, "Drop-off Zip"] not in info_locker.under_110st_zip)) or \
                        ((cd_df.ix[i, "Pick-up Zip"] not in info_locker.under_110st_zip) and (cd_df.ix[i, "Drop-off Zip"] in info_locker.under_110st_zip) and (cd_df.ix[i, "Drop-off Zip"] not in info_locker.zip_across_110)) or\
                        ((cd_df.ix[i, "Pick-up Zip"] not in info_locker.under_110st_zip) and (cd_df.ix[i, "Drop-off Zip"] in info_locker.zip_across_110) and (is_drop_under_110==True)):
                        # ((cd_df.ix[i, "Pick-up Zip"] not in info_locker.zip_across_110) and (is_drop_under_110 == False))

                    if cd_df.ix[i, "Code2"].__len__() == 0:
                        cd_df.ix[i, "Code2"] = "A0100"
                        cd_df.ix[i, "Code2 Modifier"] = "SC"

                    elif (cd_df.ix[i, "Code2"]!="A0100" and cd_df.ix[i, "Code2 Modifier"] != 'SC'):
                        cd_df.ix[i, "Code3"] = "A0100"
                        cd_df.ix[i, "Code3 Modifier"] = "SC"

        temp_df['service_date'] = cd_df['Service Starts'].apply(lambda x: datetime.strptime(x, '%m/%d/%Y').date())
        min_service_date = min(temp_df['service_date'])
        max_service_date = max(temp_df['service_date'])

        cd_df = cd_df[['Export ID', 'Record_Number', 'Invoice Number', 'Record Type',
                           'First Name', 'Middle Initial', 'Last Name', 'CIN', 'Gender',
                           'Telephone', 'Birthdate', 'Medical Provider', 'Provider ID',
                           'Ordering Provider ID', 'Transport Company', 'Transport Type',
                           'Procedure Code', 'Procedure Code Modifier', 'Service Starts',
                           'Service Ends', 'Standing Order', 'Trips Approved', 'Days Approved',
                           'Wheelchair', 'Contact Name', 'Contact Phone',
                           'Total/Calculated Mileage', 'Pick-up Date', 'Pick-up Time',
                           'Pick-up Address', 'Pick-up Ste/Apt', 'Pick-up City', 'Pick-up State',
                           'Pick-up Zip', 'Drop-off Date', 'Drop-off Time', 'Drop-off Address',
                           'Drop-off Ste/Apt', 'Drop-off City', 'Drop-off State', 'Drop-off Zip',
                           'Leg Mileage', 'Instructions', 'Secondary Service', 'Changed', 'Leg ID',
                           'Code1', 'Code1 Modifier', 'Code2', 'Code2 Modifier', 'Code3', 'Code3 Modifier']]
        if tofile == True:
            cd_df.to_excel(os.path.join(file_saving_path, 'Processed MAS-{0}-to-{1}.xlsx'.format(min_service_date, max_service_date)), index=False)

        return cd_df


class SignoffAndCompare():
    def __init__(self):
        pass

    def sign_off(self, mas_2, total_job, tofile=False):         # both are excel files

        temp_df = pd.DataFrame()
        total_job_df = pd.read_csv(total_job, header=None) if total_job[-1] == 'v' else pd.read_excel(total_job, header=None)
        total_job_df.columns = ['FleetNumber', 'ServiceDate', 'CompanyCode', 'CustomerName', 'TripID', 'TollFee',
                                'Amount', 'pComany', 'pReserve', 'pPerson']
        total_job_df = total_job_df.dropna()

        # check if there is duplicated trip in total jobs
        duplicated_idx = total_job_df.duplicated(subset=['TripID'], keep='last')
        if any(duplicated_idx):
            only_duplicated_trips_in_totaljobs = total_job_df.loc[duplicated_idx]
            only_duplicated_trips_in_totaljobs.to_excel(os.path.join(file_saving_path, 'duplicated_trips_in_totaljob-{0}-{1}.xlsx'.format(str(datetime.today().date()), str(datetime.now().time().strftime('%H%M%S')))), index=False)


        if isinstance(mas_2, pd.DataFrame):
            mas_2_df = mas_2
        else:
            mas_2_df = pd.read_csv(mas_2) if mas_2[-1] == 'v' else pd.read_excel(mas_2)

        #### drop service type
        service_idx = mas_2_df.loc[mas_2_df['Record Type'] == 'Service'].index
        mas_2_df = mas_2_df.drop(mas_2_df.index[service_idx])

            ### Merge code
        temp_df['merge_code1'] = mas_2_df['Code1'].fillna("") + mas_2_df['Code1 Modifier'].fillna("")
        temp_df['merge_code2'] = mas_2_df['Code2'].fillna("") + mas_2_df['Code2 Modifier'].fillna("")
        temp_df['merge_code3'] = mas_2_df['Code3'].fillna("") + mas_2_df['Code3 Modifier'].fillna("")

        mas_2_df['merge_codes'] = temp_df['merge_code1'] + " "+ temp_df['merge_code2'] + " " + temp_df['merge_code3']
        mas_2_df['merge_codes'] = mas_2_df['merge_codes'].apply(lambda x: str(x).strip().replace(" ", ","))

            ##### clean address
        mas_2_df['Pick-up Address'] = mas_2_df['Pick-up Address'].apply(lambda x: Process_Method().clean_address(x))
        mas_2_df['Drop-off Address'] = mas_2_df['Drop-off Address'].apply(lambda x: Process_Method().clean_address(x))

            ##### add driver info to total jobs
        total_job_df['driver id'] = total_job_df['FleetNumber'].apply(lambda x: info_locker.driver_information[x]['DRIVER_ID'])
        total_job_df['vehicle id'] = total_job_df['FleetNumber'].apply(lambda x: info_locker.driver_information[x]['VEHICLE_ID'])

            ##### compute difference
        data_in_total = mas_2_df[mas_2_df['Invoice Number'].isin(total_job_df['TripID'])]
        # data_not_in_total = mas_2_df[~mas_2_df['Invoice Number'].isin(total_job_df['TripID'])]

        invoice_list_in_total = data_in_total['Invoice Number'].tolist()
        # invoice_list_not_in_total = data_not_in_total['Invoice Number'].tolist()

        total_job_df['Codes'] = ""
        total_job_df['MAS amount'] = ""
        total_job_df['Difference'] = ""
        #total job compares MAS amount
        unique_trip_in_totaljob = total_job_df['TripID'].unique().tolist()
        for trip in unique_trip_in_totaljob:
            idx_trip_totaljob = total_job_df.loc[total_job_df['TripID'] == trip].index.tolist()
            idx_trip_processedjob = mas_2_df.loc[mas_2_df['Invoice Number'] == trip].index.tolist()

            mas_amount = 0.
            if idx_trip_processedjob.__len__() != 0:
                mas_merge_codes = mas_2_df.ix[idx_trip_processedjob[0], 'merge_codes']
                leg_mile = float(mas_2_df.ix[idx_trip_processedjob[0], 'Leg Mileage'])
                total_job_df.ix[idx_trip_totaljob[0], 'Codes'] = mas_merge_codes
                # print(mas_merge_codes)
                for code in mas_merge_codes.split(','):   ######## BUG: mas_merge_codes is string, should split first
                    if code == 'A0100':
                        mas_amount += 25.95
                    elif code == 'A0100TN':
                        mas_amount += 35
                    elif code == 'S0215':
                        mas_amount += 3.21 * (leg_mile - 8.)
                    elif code == 'S0215TN':
                        mas_amount += 2.25*leg_mile
                    elif code == 'A0100SC':
                        mas_amount += 25
                    else:
                        mas_amount += 0

                total_job_df.ix[idx_trip_totaljob[0], 'MAS amount'] = math.floor(float(format(mas_amount * 100, '.2f'))) / 100.0
                total_job_df.ix[idx_trip_totaljob[0], 'Difference'] = mas_amount - total_job_df.ix[idx_trip_totaljob[0], 'Amount']

        total_job_df.to_excel(os.path.join(file_saving_path, f'Difference_Totaljobs&Claims_{datetime.today().date()}_{datetime.now().time().strftime("%H%M%S")}.xlsx'), index=False)


        sign_off_df = pd.DataFrame()

        def get_tollfee(x):  # get toll fee from total jobs file
            res = total_job_df.loc[total_job_df['TripID'] == x, 'TollFee'].iloc
            try:
                result = res[0]
            except:
                result = 0
            return result

        def get_leg_status(x):    # if invoice number in total jobs, assign leg status 0
            # print(x)
            if x in invoice_list_in_total:

                return 0
            else:
                return 1

        def get_driver_id(x):
            res = total_job_df.loc[total_job_df['TripID'] == x, 'driver id'].iloc

            try:
                result = res[0]

            except:
                result = ""

            return result

        def get_vehicle_id(x):
            res = total_job_df.loc[total_job_df['TripID']==x, 'vehicle id'].iloc

            try:
                result = res[0]

            except:
                result = ""

            return result

        def transfer2time(x):
            try:
                result = datetime.strptime(x, '%H%M').strftime('%H:%M')

            except:
                result = 'CA:LL'

            return result

        sign_off_df['SERVICE DAY'] = mas_2_df['Service Starts']
        sign_off_df['INVOICE ID'] = mas_2_df['Invoice Number']
        sign_off_df['LEG ID'] = mas_2_df['Leg ID'].astype(int)
        sign_off_df['TOLL FEE'] = sign_off_df['INVOICE ID'].apply(lambda x: get_tollfee(x))
        sign_off_df['PROCEDURE CODE'] = mas_2_df['merge_codes']
        sign_off_df['TRIP MILEAGE'] = mas_2_df['Leg Mileage']
        sign_off_df['PICK UP ADDRESS'] = mas_2_df['Pick-up Address']
        sign_off_df['PICK UP CITY'] = mas_2_df['Pick-up City']
        sign_off_df['PICK UP ZIPCODE'] = mas_2_df['Pick-up Zip'].astype(int)
        sign_off_df['DROP OFF ADDRESS'] = mas_2_df['Drop-off Address']
        sign_off_df['DROP OFF CITY'] = mas_2_df['Drop-off City']
        sign_off_df['DROP OFF ZIPCODE'] = mas_2_df['Drop-off Zip'].astype(int)
        sign_off_df['PICK UP TIME'] = mas_2_df['Pick-up Time']
        sign_off_df['PICK UP TIME'] = sign_off_df['PICK UP TIME'].apply(lambda x: str(x).zfill(4))
        sign_off_df['PICK UP TIME'] = sign_off_df['PICK UP TIME'].apply(lambda x: transfer2time(x))

        def transfer2timeformat(x):
            try:
                result = datetime.strptime(x, '%H:%M').time()
            except:
                result = datetime.strptime('00:00', '%H:%M').time()
            return result

        temp_df['delta_time'] = sign_off_df['TRIP MILEAGE'].apply(lambda x: timedelta(minutes=(int(x)+0.4) * 4))
        temp_df['pick_up time'] = sign_off_df['PICK UP TIME'].apply(lambda x: transfer2timeformat(x))
        temp_df['dropoff_time1'] = temp_df['pick_up time'].apply(lambda x: datetime.combine(datetime.today().date(), x))
        temp_df['dropoff_time'] = temp_df['dropoff_time1'] + temp_df['delta_time']
        temp_df['dropoff_time'] = temp_df['dropoff_time'].apply(lambda x: x.time().strftime('%H:%M'))
        sign_off_df['DROP OFF TIME'] = temp_df['dropoff_time']
        sign_off_df['DRIVER ID'] = sign_off_df['INVOICE ID'].apply(lambda x: get_driver_id(x))
        sign_off_df['VEHICLE ID'] = sign_off_df['INVOICE ID'].apply(lambda x: get_vehicle_id(x))
        sign_off_df['LEG STATUS'] = sign_off_df['INVOICE ID'].apply(lambda x: get_leg_status(x))

        data_not_in_total = total_job_df[~total_job_df['TripID'].isin(sign_off_df['INVOICE ID'])]
        if data_not_in_total.__len__() != 0:
            data_not_in_total.to_excel(os.path.join(file_saving_path, 'Missed Trips in TotalJob but not in signoff-' + str(datetime.today().date()) + str(datetime.now().time().strftime("%H%M%S")) + '.xlsx'), index=False)

        unique_invoice = sign_off_df["INVOICE ID"].unique().tolist()


        ####################################### change CALL on pick up or drop off time##############
        pickup_delta = timedelta(minutes=45)

        for invoice_num in unique_invoice:
            idx = sign_off_df.loc[sign_off_df["INVOICE ID"] == invoice_num].index.tolist()
            leg_id = [sign_off_df.ix[i, 'LEG ID'] for i in idx]
            leg_id.sort()

            sorted_idx = [sign_off_df.loc[sign_off_df['LEG ID'] == l].index[0] for l in leg_id]

            for index, i in enumerate(sorted_idx):
                if sign_off_df.ix[i, 'PICK UP TIME'] == 'CA:LL' and index != 0:
                    last_row_idx = sorted_idx[index - 1]
                    last_row_dropoff_time = sign_off_df.ix[last_row_idx, "DROP OFF TIME"]
                    temp_last_dropoff_time = datetime.strptime(last_row_dropoff_time, "%H:%M").time()
                    temp_last_dropoff_time_date = datetime.combine(datetime.today().date(), temp_last_dropoff_time)

                    new_pickup_time = datetime.combine(datetime.today().date(), temp_last_dropoff_time) + pickup_delta
                    new_dropoff_time = new_pickup_time + pickup_delta

                    if temp_last_dropoff_time_date.date() == new_pickup_time.date():
                        sign_off_df.ix[i, 'PICK UP TIME'] = new_pickup_time.strftime("%H:%M")
                        sign_off_df.ix[i, 'DROP OFF TIME'] = new_dropoff_time.strftime("%H:%M")
                    else:
                        pass

        temp_df['service_date'] = mas_2_df['Service Starts'].apply(lambda x: datetime.strptime(x, '%m/%d/%Y').date())
        self.min_service_date = min(temp_df['service_date'])
        self.max_service_date = max(temp_df['service_date'])

        missed_trip_concat_to_signoff_df = pd.DataFrame()
        missed_trip_concat_to_signoff_df['SERVICE DAY'] = data_not_in_total['ServiceDate']
        missed_trip_concat_to_signoff_df['INVOICE ID'] = data_not_in_total['TripID']
        missed_trip_concat_to_signoff_df['LEG ID'] = "NA"
        missed_trip_concat_to_signoff_df['PROCEDURE CODE'] = "NA"
        missed_trip_concat_to_signoff_df['TRIP MILEAGE'] = "NA"
        missed_trip_concat_to_signoff_df['PICK UP ADDRESS'] = "NA"
        missed_trip_concat_to_signoff_df['PICK UP CITY'] = "NA"
        missed_trip_concat_to_signoff_df['PICK UP ZIPCODE'] = "NA"
        missed_trip_concat_to_signoff_df['DROP OFF ADDRESS'] = "NA"
        missed_trip_concat_to_signoff_df['DROP OFF CITY'] = "NA"
        missed_trip_concat_to_signoff_df['DROP OFF ZIPCODE'] = "NA"
        missed_trip_concat_to_signoff_df['PICK UP TIME'] = "NA"
        missed_trip_concat_to_signoff_df['DROP OFF TIME'] = "NA"
        missed_trip_concat_to_signoff_df['DRIVER ID'] = data_not_in_total['driver id']
        missed_trip_concat_to_signoff_df['VEHICLE ID'] = data_not_in_total['vehicle id']
        missed_trip_concat_to_signoff_df['LEG STATUS'] = "NA"
        missed_trip_concat_to_signoff_df['CIN'] = "NA"

        sign_off_df = sign_off_df.sort_values(by='LEG STATUS')
        sign_off_df['INVOICE ID'] = sign_off_df['INVOICE ID'].apply(lambda x: x[:-1])
        sign_off_df['CIN'] = mas_2_df['CIN']

        sign_off_df = pd.concat([sign_off_df, missed_trip_concat_to_signoff_df], 0)
        sign_off_df = sign_off_df[['SERVICE DAY', 'INVOICE ID', 'LEG ID', 'TOLL FEE', 'PROCEDURE CODE',
                                   'TRIP MILEAGE', 'PICK UP ADDRESS', 'PICK UP CITY', 'PICK UP ZIPCODE',
                                   'DROP OFF ADDRESS', 'DROP OFF CITY', 'DROP OFF ZIPCODE', 'PICK UP TIME',
                                   'DROP OFF TIME', 'DRIVER ID', 'VEHICLE ID', 'LEG STATUS', 'CIN']]
        if tofile == True:
            # date = datetime.today().date()
            sign_off_df.to_excel(os.path.join(file_saving_path, 'MAS Sign-off-{0}-to-{1}.xlsx'.format(self.min_service_date, self.max_service_date)),index=False)

        return sign_off_df

    def compare_signoff_PA(self, signoff, pa_file, tofile=False, to837=False, mas_2=None):
        '''
        Encode unit or Qty as order: [A0100, A0100TN, A0215, A0215TN, A0100SC, A0170CG]
        :param signoff: Sign-off csv file name
        :param pa_file: PA-Roast csv file name
        :param tofile: If generate output csv file
        :return:
        '''
        temp_df = pd.DataFrame()
        result_df = pd.DataFrame()
        df_for_837 = pd.DataFrame()
        missed_trip = pd.DataFrame()

        pa_roast_df = pd.read_csv(pa_file) if pa_file[-1] == 'v' else pd.read_table(pa_file)
        pa_roast_df = pa_roast_df.fillna("")
        pa_roast_df['Invoice Number'] = pa_roast_df['Invoice Number'].astype(str)

        signoff_df = pd.read_csv(signoff) if signoff[-1] == 'v' else pd.read_excel(signoff)
        signoff_df = signoff_df.loc[signoff_df['LEG STATUS'] == 0]

        # unique_invoice_pa = pa_roast_df['Invoice Number'].unique().tolist()
        unique_invoice_signoff = signoff_df['INVOICE ID'].unique().tolist()

        invoice_num_list = []
        encode_pa = []
        pa_number = []
        encode_signoff = []
        signoff_Amount_notollfee = []
        service_date_list = []
        signoff_tollfee_list = []
        CIN_list = []
        missed_trips_list = []
        service_NPI = []
        driver_id = []
        vehicle_id = []

        for invoice_num in unique_invoice_signoff:
            ####### process PA roast #########

            idx_pa = pa_roast_df.loc[pa_roast_df['Invoice Number'] == str(invoice_num)].index.tolist()
            if idx_pa.__len__() == 0:
                missed_trips_list.append(invoice_num)

            else:

                pa_number.append(pa_roast_df.ix[idx_pa[0], 'Prior Approval Number'])  # append pa number
                service_NPI.append(pa_roast_df.ix[idx_pa[0], 'Ordering Provider'])
                invoice_num_list.append(invoice_num)
                temp_encode_pa = [0 for _ in range(5)]
                for i in idx_pa:

                    code = pa_roast_df.ix[i, 'Item Code'] + pa_roast_df.ix[i, 'Item Code Mod']
                    unit = pa_roast_df.ix[i, 'Qty']
                    if code == 'A0100':
                        temp_encode_pa[0] = int(unit)
                    elif code == 'A0100TN':
                        temp_encode_pa[1] = int(unit)
                    elif code == 'S0215':
                        temp_encode_pa[2] = unit
                    elif code == 'S0215TN':
                        temp_encode_pa[3] = unit
                    elif code == 'A0100SC':
                        temp_encode_pa[4] = int(unit)
                    # elif code == 'A0170CG': temp_encode[5] = unit
                    else:
                        pass

                encode_pa.append(temp_encode_pa)

                ######## process sign off #############
                idx_sign = signoff_df.loc[signoff_df['INVOICE ID'] == invoice_num].index.tolist()
                service_date_list.append(signoff_df.ix[idx_sign[0], 'SERVICE DAY'])
                CIN_list.append(signoff_df.ix[idx_sign[0], 'CIN'])
                driver_id.append(signoff_df.ix[idx_sign[0], 'DRIVER ID'])
                vehicle_id.append(signoff_df.ix[idx_sign[0], 'VEHICLE ID'])
                temp_encode_signoff = [0 for _ in range(5)]
                all_codes = []
                all_0215mileage = []
                all_0215TN_mileage = []
                signoff_tollfee = []

                for i in idx_sign:
                    code = signoff_df.ix[i, 'PROCEDURE CODE']
                    trip_mile = signoff_df.ix[i, 'TRIP MILEAGE']
                    code = code.split(",")

                    if 'S0215' in code:
                        all_0215mileage.append(trip_mile - 8)
                    if 'S0215TN' in code:
                        all_0215TN_mileage.append(trip_mile)

                    all_codes += code  # update all codes in one invoice number
                    signoff_tollfee.append(signoff_df.ix[i, 'TOLL FEE'])
                toll_fee = sum(signoff_tollfee)

                ######## compute unit #############
                temp_encode_signoff[0] = all_codes.count('A0100')
                temp_encode_signoff[1] = all_codes.count('A0100TN')
                temp_encode_signoff[2] = round(sum(all_0215mileage), 2) if 'S0215' in all_codes else 0
                temp_encode_signoff[3] = round(sum(all_0215TN_mileage), 2) if 'S0215TN' in all_codes else 0
                temp_encode_signoff[4] = all_codes.count('A0100SC')
                # temp_encode[5] = all_codes.count('A0170CG')
                encode_signoff_array = np.asarray(temp_encode_signoff)
                price_array = np.array([[25.95], [35], [3.21], [2.25], [25]])
                totalprice = np.dot(encode_signoff_array, price_array)
                totalprice = str(totalprice)
                totalprice = float(totalprice[1:-1])
                totalprice = math.floor(float(format(totalprice * 100, '.2f'))) / 100.0

                signoff_Amount_notollfee.append(totalprice)
                encode_signoff.append(temp_encode_signoff)
                signoff_tollfee_list.append(toll_fee)

        missed_trip['MISSED TRIPS'] = missed_trips_list

        result_df['service_date'] = service_date_list
        result_df['invoice number'] = invoice_num_list
        result_df['CIN'] = CIN_list
        result_df['pa_number'] = pa_number
        result_df['DRIVER ID'] = driver_id
        result_df['VEHICLE ID'] = vehicle_id
        result_df['Service NPI'] = service_NPI
        result_df['encode_pa'] = encode_pa
        result_df['encode_signoff'] = encode_signoff
        result_df['compare_result'] = np.where(result_df['encode_pa'] == result_df['encode_signoff'], "", "Different")
        result_df['sign-off amount no toll fee'] = signoff_Amount_notollfee
        result_df['sign-off toll fee'] = signoff_tollfee_list
        result_df['sign-off Total Amount'] = result_df['sign-off amount no toll fee'] + result_df['sign-off toll fee']
        # result_df['sign-off Total Amount'] = result_df['sign-off Total Amount'].apply(lambda x: math.floor(x * 100) / 100.0)

        temp_df['service_date'] = signoff_df['SERVICE DAY'].apply(lambda x: datetime.strptime(x, '%m/%d/%Y').date())
        self.min_service_date = min(temp_df['service_date'])
        self.max_service_date = max(temp_df['service_date'])

        if tofile == True:
            result_df.to_excel(os.path.join(file_saving_path, 'MAS Correction-{0}-to-{1}.xlsx'.format(self.min_service_date, self.max_service_date)), index=False)

        if missed_trip.__len__() != 0:
            missed_trip.to_excel(os.path.join(file_saving_path, 'MISSED TRIPS-{0}-to-{1}.xlsx'.format(self.min_service_date, self.max_service_date)), index=False)

        if to837 == True:
            mas_2_df = pd.read_csv(mas_2) if mas_2[-1] == 'v' else pd.read_excel(mas_2)

            patient_lastname = []
            patient_firstname = []
            patient_address = []
            patient_city = []
            patient_state = []
            patient_zipcode = []
            patient_gender = []
            patient_pregnant = []
            patient_dob = []
            patient_medicaid_num = []
            invoice_number_for837 = []
            pa_number_for837 = []
            driver_lastname = []
            driver_firstname = []
            driver_license = []
            vehicle_plate = []
            service_facility_name_list = []
            service_address_list = []
            service_city = []
            service_state = []
            service_zip = []
            service_date = []
            service_npi = []
            claim_amount = []

            code1 = []
            code1_modifier = []
            code1_amount = []
            code1_unit = []

            code2 = []
            code2_modifier = []
            code2_amount = []
            code2_unit = []

            code3 = []
            code3_modifier = []
            code3_amount = []
            code3_unit = []

            code4 = []
            code4_modifier = []
            code4_amount = []
            code4_unit = []

            code5 = []
            code5_modifier = []
            code5_amount = []
            code5_unit = []

            code6 = []
            code6_modifier = []
            code6_amount = []
            code6_unit = []

            for invoice_num in unique_invoice_signoff:
                invoice_num_in_mas = str(invoice_num) + 'A'  # Get A leg in mas_2_df
                invoice_num_in_mas_index = mas_2_df.loc[mas_2_df['Invoice Number'] == invoice_num_in_mas].index.tolist()

                invoice_num_in_result_df_index = result_df.loc[result_df['invoice number'] == invoice_num].index.tolist()
                invoice_num_in_pa_roast_index = pa_roast_df.loc[pa_roast_df['Invoice Number'] == invoice_num].index.tolist()
                if invoice_num_in_pa_roast_index.__len__() == 0:
                    continue
                invoice_num_in_signoff_index = signoff_df.loc[signoff_df['INVOICE ID'] == invoice_num].index.tolist()

                patient_firstname.append(mas_2_df.ix[invoice_num_in_mas_index[0], 'First Name'].upper())
                patient_lastname.append(mas_2_df.ix[invoice_num_in_mas_index[0], 'Last Name'].upper())
                patient_address.append(mas_2_df.ix[invoice_num_in_mas_index[0], 'Pick-up Address'])
                patient_city.append(mas_2_df.ix[invoice_num_in_mas_index[0], 'Pick-up City'])
                patient_state.append(mas_2_df.ix[invoice_num_in_mas_index[0], 'Pick-up State'])
                patient_zipcode.append(mas_2_df.ix[invoice_num_in_mas_index[0], 'Pick-up Zip'])
                patient_gender.append(mas_2_df.ix[invoice_num_in_mas_index[0], 'Gender'])
                patient_pregnant.append('N')
                patient_dob.append(mas_2_df.ix[invoice_num_in_mas_index[0], 'Birthdate'])
                patient_medicaid_num.append(mas_2_df.ix[invoice_num_in_mas_index[0], 'CIN'])
                invoice_number_for837.append(invoice_num)

                temp_pa_number = re.findall(r'\d+', pa_roast_df.ix[invoice_num_in_pa_roast_index[0], 'Prior Approval Number'])
                if temp_pa_number.__len__() == 0:
                    temp_pa_number = [0]
                pa_number_for837.append(temp_pa_number[0])

                driver_license_num = signoff_df.ix[invoice_num_in_signoff_index[0], 'DRIVER ID']
                vehicle_plate_num = signoff_df.ix[invoice_num_in_signoff_index[0], 'VEHICLE ID']
                driver_fn, driver_ln = Process_Method().use_driver_id_to_find_drivername(int(driver_license_num))

                driver_lastname.append(driver_ln)
                driver_firstname.append(driver_fn)
                driver_license.append(int(driver_license_num))
                vehicle_plate.append(vehicle_plate_num)

                service_facility_name_list.append(str(mas_2_df.ix[invoice_num_in_mas_index[0], 'Medical Provider']).replace(',', ''))
                service_address_list.append(mas_2_df.ix[invoice_num_in_mas_index[0], 'Drop-off Address'])
                service_city.append(mas_2_df.ix[invoice_num_in_mas_index[0], 'Drop-off City'])
                service_state.append(mas_2_df.ix[invoice_num_in_mas_index[0], 'Drop-off State'])
                service_zip.append(mas_2_df.ix[invoice_num_in_mas_index[0], 'Drop-off Zip'])
                service_date.append(mas_2_df.ix[invoice_num_in_mas_index[0], 'Service Starts'])
                service_npi.append(mas_2_df.ix[invoice_num_in_mas_index[0], 'Ordering Provider ID'])

                claim_amount.append(result_df.ix[invoice_num_in_result_df_index[0], 'sign-off Total Amount'])

                #### for service codes
                encoded_service_codes_no_toll = result_df.ix[invoice_num_in_result_df_index[0], 'encode_signoff']
                invoice_num_tollfee = result_df.ix[invoice_num_in_result_df_index[0], 'sign-off toll fee']

                encoded_service_codes_no_toll.append(invoice_num_tollfee)
                encoded_all_service_codes = encoded_service_codes_no_toll

                non_zero_encoding_index = []
                for encoding_index, qty in enumerate(encoded_all_service_codes):
                    if qty == 0:
                        continue
                    else:
                        non_zero_encoding_index.append(encoding_index)

                number_of_codes = non_zero_encoding_index.__len__()

                if number_of_codes == 1:
                    code1.append(info_locker.decoding_info[str(non_zero_encoding_index[0])]['code'])
                    code1_modifier.append(info_locker.decoding_info[str(non_zero_encoding_index[0])]['modifier'])
                    code1_qty = encoded_all_service_codes[non_zero_encoding_index[0]]
                    code1_unit.append(code1_qty)
                    code1_amount.append(math.floor(info_locker.decoding_info[str(non_zero_encoding_index[0])]['price'] * code1_qty * 100) / 100.0)

                    code2.append("")
                    code2_modifier.append("")
                    code2_amount.append("")
                    code2_unit.append("")

                    code3.append("")
                    code3_modifier.append("")
                    code3_amount.append("")
                    code3_unit.append("")

                    code4.append("")
                    code4_modifier.append("")
                    code4_amount.append("")
                    code4_unit.append("")

                    code5.append("")
                    code5_modifier.append("")
                    code5_amount.append("")
                    code5_unit.append("")

                    code6.append("")
                    code6_modifier.append("")
                    code6_amount.append("")
                    code6_unit.append("")

                if number_of_codes == 2:
                    code1.append(info_locker.decoding_info[str(non_zero_encoding_index[0])]['code'])
                    code1_modifier.append(info_locker.decoding_info[str(non_zero_encoding_index[0])]['modifier'])
                    code1_qty = encoded_all_service_codes[non_zero_encoding_index[0]]
                    code1_unit.append(code1_qty)
                    code1_amount.append(math.floor(info_locker.decoding_info[str(non_zero_encoding_index[0])]['price'] * code1_qty * 100) / 100.0)

                    if non_zero_encoding_index[1] != 5:
                        code2.append(info_locker.decoding_info[str(non_zero_encoding_index[1])]['code'])
                        code2_modifier.append(info_locker.decoding_info[str(non_zero_encoding_index[1])]['modifier'])
                        code2_qty = encoded_all_service_codes[non_zero_encoding_index[1]]
                        code2_unit.append(code2_qty)
                        code2_amount.append(math.floor(info_locker.decoding_info[str(non_zero_encoding_index[1])]['price'] * code2_qty * 100) / 100.0)

                    else:
                        code2.append(info_locker.decoding_info[str(non_zero_encoding_index[1])]['code'])
                        code2_modifier.append(info_locker.decoding_info[str(non_zero_encoding_index[1])]['modifier'])
                        code2_unit.append(1)
                        code2_amount.append(encoded_all_service_codes[5])

                    code3.append("")
                    code3_modifier.append("")
                    code3_amount.append("")
                    code3_unit.append("")

                    code4.append("")
                    code4_modifier.append("")
                    code4_amount.append("")
                    code4_unit.append("")

                    code5.append("")
                    code5_modifier.append("")
                    code5_amount.append("")
                    code5_unit.append("")

                    code6.append("")
                    code6_modifier.append("")
                    code6_amount.append("")
                    code6_unit.append("")

                if number_of_codes == 3:
                    code1.append(info_locker.decoding_info[str(non_zero_encoding_index[0])]['code'])
                    code1_modifier.append(info_locker.decoding_info[str(non_zero_encoding_index[0])]['modifier'])
                    code1_qty = encoded_all_service_codes[non_zero_encoding_index[0]]
                    code1_unit.append(code1_qty)
                    code1_amount.append(math.floor(info_locker.decoding_info[str(non_zero_encoding_index[0])]['price'] * code1_qty * 100) / 100.0)

                    code2.append(info_locker.decoding_info[str(non_zero_encoding_index[1])]['code'])
                    code2_modifier.append(info_locker.decoding_info[str(non_zero_encoding_index[1])]['modifier'])
                    code2_qty = encoded_all_service_codes[non_zero_encoding_index[1]]
                    code2_unit.append(code2_qty)
                    code2_amount.append(math.floor(info_locker.decoding_info[str(non_zero_encoding_index[1])]['price'] * code2_qty * 100) / 100.0)

                    if non_zero_encoding_index[2] != 5:
                        code3.append(info_locker.decoding_info[str(non_zero_encoding_index[2])]['code'])
                        code3_modifier.append(info_locker.decoding_info[str(non_zero_encoding_index[2])]['modifier'])
                        code3_qty = encoded_all_service_codes[non_zero_encoding_index[2]]
                        code3_unit.append(code3_qty)
                        code3_amount.append(math.floor(info_locker.decoding_info[str(non_zero_encoding_index[2])]['price'] * code3_qty * 100) / 100.0)

                    else:
                        code3.append(info_locker.decoding_info[str(non_zero_encoding_index[2])]['code'])
                        code3_modifier.append(info_locker.decoding_info[str(non_zero_encoding_index[2])]['modifier'])
                        code3_unit.append(1)
                        code3_amount.append(encoded_all_service_codes[5])

                    code4.append("")
                    code4_modifier.append("")
                    code4_amount.append("")
                    code4_unit.append("")

                    code5.append("")
                    code5_modifier.append("")
                    code5_amount.append("")
                    code5_unit.append("")

                    code6.append("")
                    code6_modifier.append("")
                    code6_amount.append("")
                    code6_unit.append("")

                if number_of_codes == 4:
                    code1.append(info_locker.decoding_info[str(non_zero_encoding_index[0])]['code'])
                    code1_modifier.append(info_locker.decoding_info[str(non_zero_encoding_index[0])]['modifier'])
                    code1_qty = encoded_all_service_codes[non_zero_encoding_index[0]]
                    code1_unit.append(code1_qty)
                    code1_amount.append(
                        math.floor(info_locker.decoding_info[str(non_zero_encoding_index[0])]['price'] * code1_qty * 100) / 100.0)

                    code2.append(info_locker.decoding_info[str(non_zero_encoding_index[1])]['code'])
                    code2_modifier.append(info_locker.decoding_info[str(non_zero_encoding_index[1])]['modifier'])
                    code2_qty = encoded_all_service_codes[non_zero_encoding_index[1]]
                    code2_unit.append(code2_qty)
                    code2_amount.append(
                        math.floor(info_locker.decoding_info[str(non_zero_encoding_index[1])]['price'] * code2_qty * 100) / 100.0)

                    code3.append(info_locker.decoding_info[str(non_zero_encoding_index[2])]['code'])
                    code3_modifier.append(info_locker.decoding_info[str(non_zero_encoding_index[2])]['modifier'])
                    code3_qty = encoded_all_service_codes[non_zero_encoding_index[2]]
                    code3_unit.append(code3_qty)
                    code3_amount.append(
                        math.floor(info_locker.decoding_info[str(non_zero_encoding_index[2])]['price'] * code3_qty * 100) / 100.0)

                    code4.append(info_locker.decoding_info[str(non_zero_encoding_index[3])]['code'])
                    code4_modifier.append(info_locker.decoding_info[str(non_zero_encoding_index[3])]['modifier'])
                    code4_qty = encoded_all_service_codes[non_zero_encoding_index[3]]
                    # code4_unit.append(code4_qty)
                    # code4_amount.append(
                    #     math.floor(decoding_info[str(non_zero_encoding_index[3])]['price'] * code4_qty * 100) / 100.0)
                    code4_unit.append(1)
                    code4_amount.append(encoded_all_service_codes[5])

                    code5.append("")
                    code5_modifier.append("")
                    code5_amount.append("")
                    code5_unit.append("")

                    code6.append("")
                    code6_modifier.append("")
                    code6_amount.append("")
                    code6_unit.append("")


                if number_of_codes == 5:
                    code1.append(info_locker.decoding_info[str(non_zero_encoding_index[0])]['code'])
                    code1_modifier.append(info_locker.decoding_info[str(non_zero_encoding_index[0])]['modifier'])
                    code1_qty = encoded_all_service_codes[non_zero_encoding_index[0]]
                    code1_unit.append(code1_qty)
                    code1_amount.append(
                        math.floor(info_locker.decoding_info[str(non_zero_encoding_index[0])]['price'] * code1_qty * 100) / 100.0)

                    code2.append(info_locker.decoding_info[str(non_zero_encoding_index[1])]['code'])
                    code2_modifier.append(info_locker.decoding_info[str(non_zero_encoding_index[1])]['modifier'])
                    code2_qty = encoded_all_service_codes[non_zero_encoding_index[1]]
                    code2_unit.append(code2_qty)
                    code2_amount.append(
                        math.floor(info_locker.decoding_info[str(non_zero_encoding_index[1])]['price'] * code2_qty * 100) / 100.0)

                    code3.append(info_locker.decoding_info[str(non_zero_encoding_index[2])]['code'])
                    code3_modifier.append(info_locker.decoding_info[str(non_zero_encoding_index[2])]['modifier'])
                    code3_qty = encoded_all_service_codes[non_zero_encoding_index[2]]
                    code3_unit.append(code3_qty)
                    code3_amount.append(
                        math.floor(info_locker.decoding_info[str(non_zero_encoding_index[2])]['price'] * code3_qty * 100) / 100.0)

                    code4.append(info_locker.decoding_info[str(non_zero_encoding_index[3])]['code'])
                    code4_modifier.append(info_locker.decoding_info[str(non_zero_encoding_index[3])]['modifier'])
                    code4_qty = encoded_all_service_codes[non_zero_encoding_index[3]]
                    code4_unit.append(code4_qty)
                    code4_amount.append(
                        math.floor(info_locker.decoding_info[str(non_zero_encoding_index[3])]['price'] * code4_qty * 100) / 100.0)

                    code5.append(info_locker.decoding_info[str(non_zero_encoding_index[4])]['code'])
                    code5_modifier.append(info_locker.decoding_info[str(non_zero_encoding_index[4])]['modifier'])
                    code4_qty = encoded_all_service_codes[non_zero_encoding_index[4]]
                    # code4_unit.append(code4_qty)
                    # code4_amount.append(
                    #     math.floor(decoding_info[str(non_zero_encoding_index[3])]['price'] * code4_qty * 100) / 100.0)
                    code5_unit.append(1)
                    code5_amount.append(encoded_all_service_codes[5])

                    code6.append("")
                    code6_modifier.append("")
                    code6_amount.append("")
                    code6_unit.append("")


            df_for_837['patient last name'] = patient_lastname
            df_for_837['patient first name'] = patient_firstname
            df_for_837['patient address'] = patient_address
            df_for_837['patient city'] = patient_city
            df_for_837['patient state'] = patient_state
            df_for_837['patient zip code'] = patient_zipcode
            df_for_837['patient gender'] = patient_gender
            df_for_837['patient pregnant'] = patient_pregnant
            df_for_837['patient dob'] = patient_dob
            df_for_837['patient medicaid number'] = patient_medicaid_num
            df_for_837['invoice number'] = invoice_number_for837
            df_for_837['pa number'] = pa_number_for837
            df_for_837['driver last name'] = driver_lastname
            df_for_837['driver first name'] = driver_firstname
            df_for_837['driver license number'] = driver_license
            df_for_837['driver plate number'] = vehicle_plate
            df_for_837['service facility name'] = service_facility_name_list
            df_for_837['service address'] = service_address_list
            df_for_837['service city'] = service_city
            df_for_837['service state'] = service_state
            df_for_837['service zip code'] = service_zip
            df_for_837['service date'] = service_date
            df_for_837['service npi'] = service_npi
            df_for_837['claim_amount'] = claim_amount

            df_for_837['service code 1'] = code1
            df_for_837['modifier code 1'] = code1_modifier
            df_for_837['amount 1'] = code1_amount
            df_for_837['unit 1'] = code1_unit

            df_for_837['service code 2'] = code2
            df_for_837['modifier code 2'] = code2_modifier
            df_for_837['amount 2'] = code2_amount
            df_for_837['unit 2'] = code2_unit

            df_for_837['service code 3'] = code3
            df_for_837['modifier code 3'] = code3_modifier
            df_for_837['amount 3'] = code3_amount
            df_for_837['unit 3'] = code3_unit

            df_for_837['service code 4'] = code4
            df_for_837['modifier code 4'] = code4_modifier
            df_for_837['amount 4'] = code4_amount
            df_for_837['unit 4'] = code4_unit

            df_for_837['service code 5'] = code5
            df_for_837['modifier code 5'] = code5_modifier
            df_for_837['amount 5'] = code5_amount
            df_for_837['unit 5'] = code5_unit

            df_for_837['service code 6'] = code6
            df_for_837['modifier code 6'] = code6_modifier
            df_for_837['amount 6'] = code6_amount
            df_for_837['unit 6'] = code6_unit


            df_for_837.to_excel(os.path.join(file_saving_path, '837P Data-for-{0}-to-{1}.xlsx'.format(self.min_service_date, self.max_service_date)), index=False)

        return result_df

    def new_compare_after_payment(self, signoff_compare_PA_file, payment_raw_file):

        new_compare_filename = re.findall(r'\d{4}-\d{2}-\d{2}-to-\d{4}-\d{2}-\d{2}', signoff_compare_PA_file)[0]
        new_compare_filename = "Check Payment-" + new_compare_filename + '.xlsx'

        def split_CIN_receipt(x):
            splited = str(x).split(" ")
            CIN, receipt_num = splited[0], splited[1]
            receipt_num = receipt_num.replace("-", "")
            return CIN, int(receipt_num)

        def reverse_minus(x):

            x = str(x)
            if x[-1] == '-':
                x = '-' + x.replace("-", '')
                return float(x)
            else:
                return float(x)

        def remove_leg_from_invoice_number(x):
            if type(x) == str:
                x = x[:-1]
                return int(x)
            else:
                return x

        ### process payment raw file first ###
        payment_df = pd.read_excel(payment_raw_file)
        payment_df.columns = ['useless', 'invoice number', 'patient name', 'CIN and receipt number', 'service date',
                              'code', 'code unit', 'claim amount', 'paid amount', 'note']


        useless_0_index = payment_df.loc[payment_df['useless'] == 0].index
        payment_df = payment_df.drop(payment_df.index[useless_0_index])

        payment_df['CIN'], payment_df['receipt number'] = zip(*payment_df['CIN and receipt number'].map(split_CIN_receipt))
        payment_df = payment_df.drop(['CIN and receipt number'], axis=1)

        payment_df['code unit'] = payment_df['code unit'].apply(lambda x: reverse_minus(x))
        payment_df['claim amount'] = payment_df['claim amount'].apply(lambda x: reverse_minus(x))
        payment_df['paid amount'] = payment_df['paid amount'].apply(lambda x: reverse_minus(x))
        # payment_df['paid amount'] = payment_df['paid amount'].apply(lambda x: math.floor(x * 100) / 100.0)

        payment_df['service date'] = payment_df['service date'].apply(lambda x: datetime.strptime(str(x), "%Y-%m-%d %H:%M:%S").date())
        payment_df['invoice number'] = payment_df['invoice number'].apply(lambda x: remove_leg_from_invoice_number(x))
        # print(payment_df['paid amount'].tolist())
        ############### Finish process payment raw data ##############################

        signoff_compare_PA_df = pd.read_excel(signoff_compare_PA_file)


        # signoff_compare_PA_df['service date'] = ""
        signoff_compare_PA_df['encode payment'] = ""
        signoff_compare_PA_df['payment paid amount'] = ""
        signoff_compare_PA_df['payer claim control number'] = ""
        # signoff_compare_PA_df['CIN'] = ""
        signoff_compare_PA_df['signoff payment compare'] = ""
        signoff_compare_PA_df['payment result'] = ""

        unique_invoice_number_payment = payment_df['invoice number'].unique().tolist()

        for invoice_number in unique_invoice_number_payment:

            idx_payment = payment_df.loc[payment_df['invoice number'] == invoice_number].index.tolist()
            idx_signoff_compare_PA = signoff_compare_PA_df.loc[signoff_compare_PA_df['invoice number'] == invoice_number].index.tolist()

            if idx_signoff_compare_PA.__len__() == 0:
                continue
            else:
                # signoff_compare_PA_df.ix[idx_signoff_compare_PA[0], 'service date'] = payment_df.ix[idx_payment[0], 'service date']
                signoff_compare_PA_df.ix[idx_signoff_compare_PA[0], 'payer claim control number'] = payment_df.ix[idx_payment[0], 'receipt number']
                # signoff_compare_PA_df.ix[idx_signoff_compare_PA[0], 'CIN'] = payment_df.ix[idx_payment[0], 'CIN']

                temp_encode_payment = [0 for _ in range(5)]
                temp_paid_amount = []

                for i in idx_payment:
                    temp_paid_amount.append(payment_df.ix[i, 'paid amount'])
                    code = payment_df.ix[i, 'code']
                    unit = payment_df.ix[i, 'code unit']
                    if unit < 0:
                        continue
                    else:
                        if code == 'A0100':
                            temp_encode_payment[0] = int(unit)
                        elif code == 'A0100TN':
                            temp_encode_payment[1] = int(unit)
                        elif code == 'S0215':
                            temp_encode_payment[2] = unit
                        elif code == 'S0215TN':
                            temp_encode_payment[3] = unit
                        elif code == 'A0100SC':
                            temp_encode_payment[4] = int(unit)
                        else:
                            pass

                signoff_compare_PA_df.ix[idx_signoff_compare_PA[0], 'encode payment'] = str((temp_encode_payment))
                signoff_compare_PA_df.ix[idx_signoff_compare_PA[0], 'payment paid amount'] = round(sum(temp_paid_amount), 2)
                # signoff_compare_PA_df.ix[idx_signoff_compare_PA[0], 'payment paid amount'] = math.floor(signoff_compare_PA_df.ix[idx_signoff_compare_PA[0], 'payment paid amount'] * 100) / 100.0

                # print(signoff_compare_PA_df.ix[idx_signoff_compare_PA[0], 'encode_signoff'], signoff_compare_PA_df.ix[idx_signoff_compare_PA[0], 'encode payment'])
                if signoff_compare_PA_df.ix[idx_signoff_compare_PA[0], 'encode_signoff'] != signoff_compare_PA_df.ix[idx_signoff_compare_PA[0], 'encode payment']:
                    signoff_compare_PA_df.ix[idx_signoff_compare_PA[0], 'signoff payment compare'] = 'Different'
                else:
                    pass

                # print(signoff_compare_PA_df.ix[idx_signoff_compare_PA[0], 'sign-off Total Amount'], signoff_compare_PA_df.ix[idx_signoff_compare_PA[0], 'payment paid amount'])
                if signoff_compare_PA_df.ix[idx_signoff_compare_PA[0], 'sign-off Total Amount'] != signoff_compare_PA_df.ix[idx_signoff_compare_PA[0], 'payment paid amount']:
                    signoff_compare_PA_df.ix[idx_signoff_compare_PA[0], 'payment result'] = 'Different'
                else:
                    signoff_compare_PA_df.ix[idx_signoff_compare_PA[0], 'payment result'] = 'OKAY'

                # if signoff_compare_PA_df.ix[idx_signoff_compare_PA[0], 'service date'] == "":
                #     signoff_compare_PA_df.ix[idx_signoff_compare_PA[0], 'payment result'] = 'Not Found'
                # else:
                #     pass

        signoff_compare_PA_df = signoff_compare_PA_df.fillna("")

        none_encode_payment_idx = signoff_compare_PA_df.loc[signoff_compare_PA_df['encode payment'] == ""].index.tolist()
        for i in none_encode_payment_idx:
            signoff_compare_PA_df.ix[i, 'payment result'] = 'Not Found'

        ordered_columns = ['service_date', 'invoice number', 'pa_number', 'encode_pa', 'encode_signoff', 'compare_result', 'encode payment',
                                         'signoff payment compare', 'sign-off amount no toll fee', 'sign-off toll fee', 'sign-off Total Amount', 'payment paid amount',
                                         'payment result', 'payer claim control number', 'CIN', 'DRIVER ID', 'VEHICLE ID', 'Service NPI']
        signoff_compare_PA_df = signoff_compare_PA_df[ordered_columns]

        signoff_compare_PA_df.to_excel(os.path.join(file_saving_path, new_compare_filename), index=False)


class Process_Method():

    def __init__(self):
        pass

    @staticmethod
    def use_driver_id_to_find_drivername(driverid):
        for key, value in info_locker.driver_information.items():

            if value['DRVER_ID'] == driverid:
                return value['FirstName'], value['LastName']

    @staticmethod
    def is_in_nassau(address):
        google_geocode_api_url = "https://maps.googleapis.com/maps/api/geocode/json"
        key_list = ['AIzaSyCJ69KvhuscmlIgr5IqyOideByOqJzZHcs', 'AIzaSyA-2V1w_acgbN4RO-40e2HJiwnzuMFtrrQ',
                    'AIzaSyA0b1WxrDmzoJFBuD6zua4CfVXJn1tvgko', 'AIzaSyB3K9wP-0U5EB2AeHsZIN4K5bk0MCBSW2s',
                    'AIzaSyD0OasuP_KjPwlSAc3kZrU8o4zLRh_bsrM', 'AIzaSyCMdT7Q3a178rNw6KqDt9jp8SSgud5V5gM',
                    'AIzaSyC-Axh7DKF4GGBkPYpOVrAP3IsAOpwRHkk', 'AIzaSyB_L0jCnP6hdPg8CDxIDnKp6YKGAZ7eQFM',
                    'AIzaSyAK5T_kCyb1r8aft0sRhy3KBZ1E5N4kcNM']
        random_index = random.randint(0, 8)
        # print(random_index)
        params = {
            "address": address,
            "key": key_list[random_index],
                  }

        req = requests.get(google_geocode_api_url, params=params)
        res = req.json()
        result = res['results']
        text = str(result[0]['address_components'])
        # print(text)
        return 'Nassau County' in text

    @staticmethod
    def add2geo(address):
        geo = None
        count = 0

        while geo == None:
            geo = geocoder.google(address).latlng
            count += 1
            if count > 10:
                geo = [-1, -1]
                break
            sleep(0.1)
        return geo[0], geo[1]

    @staticmethod
    def google2geo(address):
        google_geocode_api_url = "https://maps.googleapis.com/maps/api/geocode/json"
        key_list = ['AIzaSyCJ69KvhuscmlIgr5IqyOideByOqJzZHcs', 'AIzaSyA-2V1w_acgbN4RO-40e2HJiwnzuMFtrrQ',
                    'AIzaSyA0b1WxrDmzoJFBuD6zua4CfVXJn1tvgko', 'AIzaSyB3K9wP-0U5EB2AeHsZIN4K5bk0MCBSW2s',
                    'AIzaSyD0OasuP_KjPwlSAc3kZrU8o4zLRh_bsrM', 'AIzaSyCMdT7Q3a178rNw6KqDt9jp8SSgud5V5gM',
                    'AIzaSyC-Axh7DKF4GGBkPYpOVrAP3IsAOpwRHkk', 'AIzaSyB_L0jCnP6hdPg8CDxIDnKp6YKGAZ7eQFM',
                    'AIzaSyAK5T_kCyb1r8aft0sRhy3KBZ1E5N4kcNM']
        random_index = random.randint(0, 8)
        # print(random_index)
        params = {
            "address": address,
            "key": key_list[random_index],
        }
        req = requests.get(google_geocode_api_url, params=params)
        res = req.json()
        result = res['results']
        geometry = result[0]['geometry']['location']
        return geometry['lat'], geometry['lng']

    @staticmethod
    def is_under_110st(geo_x, geo_y):
        fn = -2.38056214 * geo_x + 23.1700644

        if fn > geo_y:
            return True
        elif fn < geo_y:
            return False
        else:
            print("On 110st!")

    @staticmethod
    def clean_address(x):
        if type(x) is float:
            return

        else:
            x = x.upper()
            a = x.replace(".", " ").split(" ")
            for i in a:
                if i in ['AV', 'AVE', 'AVENUE', 'BLD', 'BOULEVARD', 'BLVD', 'BLDG', 'BOWERY', 'BROADWAY', 'CI',
                         'CT', 'CIR', 'DR', 'DRIVER', 'EXP', 'EXPY', 'EXPRESSWAY', 'EXPWY', 'EXWY',
                         'HIGHWAY', 'HWY', 'LN', 'PL', 'PI', 'PARKWAY', 'PLACE', 'PLZ', 'PKWY', 'RD', 'ROAD',
                         'SQ', 'STR', 'ST', 'STREET', 'SQUARE', 'TNPK', 'TPKE', 'TURNPIKE', 'WAY', 'MALL']:
                    del a[a.index(i) + 1:]
            return " ".join(a)

    @staticmethod
    def transfer2lines(input_file, add_sep="~"):
        df = pd.read_csv(input_file, delimiter="~", header=None,)
        df = df.transpose()
        df.columns = ['line']
        df['line'] = df['line'].dropna().apply(lambda x: x + add_sep)
        inputfile_name = input_file.split("/")[-1]
        df.to_csv("Lined-"+inputfile_name, index=False, header=None)

    @staticmethod
    def transfer2stream(input_file, add_delimiter=False):
        df = pd.read_csv(input_file, names=['lines'])

        df_list = df.ix[:, 0].tolist()
        if add_delimiter == True:
            df_list = map(lambda x: x + "~", df_list)

        result = ''.join(df_list)
        inputfile_name = input_file.split("/")[-1]
        with open('Stream-' + inputfile_name, 'w') as f:
            f.write(result)

    @staticmethod
    def write_txt(data, output_file):
        with open(output_file, 'w') as f:
            f.write(data)

    @staticmethod
    def get_receipt_code(receipt_file, lined_file=True):
        if lined_file==False:    # for raw receipt data
            receipt_df = pd.read_csv(receipt_file, delimiter="~", header=None,)
            receipt_df = receipt_df.transpose()
            receipt_df.columns = ['line']
            receipt_df['line'] = receipt_df['line'].dropna().apply(lambda x: x)
        else:          # already split into lines
            receipt_df = pd.read_csv(receipt_file)
            receipt_df.columns = ['line']

        receipt_df = receipt_df.dropna()
        receipt_df['line_sep'] = receipt_df['line'].apply(lambda x: x.split("*"))

        res_dict = {}
        temp_dict = {}
        temp_inv = ""
        temp_receipt = ""

        for l in range(receipt_df.__len__()):
            row = receipt_df.ix[l, 1]

            if row[0] == 'BHT':
                temp_inv = row[3]

            if row[0] == "REF" and row[1] == "1K":
                temp_receipt = row[2]

            temp_dict[str(temp_inv)] = temp_receipt
            res_dict.update(temp_dict)

            if temp_dict.__len__() >= 1:
                temp_dict = {}

        res_dict.pop("")

        return res_dict

    @staticmethod
    def process_270_receipt(receipt_file, lined_file=True):
        '''
        :param receipt_file: receipt file from EDI270
        :param lined_file: If the file is separated by line
        :return: pandas dataframe containing eligibilty info
        '''

        SQ = mysqlite('EDI.db')   # connect to sqlite3

        if lined_file==False:    # for raw receipt data
            receipt_df = pd.read_csv(receipt_file, delimiter="~", header=None,)
            receipt_df = receipt_df.transpose()
            receipt_df.columns = ['line']
            receipt_df['line'] = receipt_df['line'].dropna()
        else:          # already split into lines
            receipt_df = pd.read_csv(receipt_file)
            receipt_df.columns = ['line']

        receipt_df = receipt_df.dropna()
        receipt_df['line_sep'] = receipt_df['line'].apply(lambda x: x.split("*"))
        # print(receipt_df)

        result_dict = {}
        temp_dict = {}
        invoice_num = ""
        patient_lastname = ""
        patient_firstname = ""
        CIN = ""
        dob = ""
        gender = ""
        service_date = ""
        eligible_code = ""
        payer_name = ""
        plan_code = ""
        address = ""
        # contact_name = ""
        contact_tel = ""
        covered_service_codes = []
        other_payer_name = []
        other_payer_address = []
        other_payer_policy_number = ""
        other_payer_telephone = []
        other_payer_group_number = []

        other_payer_name1 = ""
        other_payer_address1 = ""
        other_payer_telephone1 = ""
        other_payer_group_number1 = ""
        other_payer_name2 = ""
        other_payer_address2 = ""
        other_payer_telephone2 = ""
        other_payer_group_number2 = ""
        eligible_result = ""

        for l in range(receipt_df.__len__()):
            row = receipt_df.ix[l, 1]

            if row[0] == 'BHT':
                invoice_num = row[3]

            elif row[0] == 'AAA' and row[1] == 'N':
                if row[3] == '15':
                    eligible_code = 'Required Application Data Missing'
                elif row[3] == '35':
                    eligible_code = 'Out of Network'
                elif row[3] == '42':
                    eligible_code = 'Unable to respond at current time'
                elif row[3] == '43':
                    eligible_code = 'Invalid/missing provider ID'
                elif row[3] == '45':
                    eligible_code = 'Invalid/missing provider specialty'
                elif row[3] == '47':
                    eligible_code = 'Invalid/missing provider state'
                elif row[3] == '48':
                    eligible_code = 'Invalid/missing referring provider ID number'
                elif row[3] == '49':
                    eligible_code = 'Provider is not primary care physician'
                elif row[3] == '51':
                    eligible_code = 'Provider not on file'
                elif row[3] == '52':
                    eligible_code = 'Service dates not within provider plan enrollment'
                elif row[3] == '56':
                    eligible_code = 'Inappropriate date'
                elif row[3] == '57':
                    eligible_code = 'Invalid/missing date of service'
                elif row[3] == '58':
                    eligible_code = 'Invalid/missing date of birth'
                elif row[3] == '60':
                    eligible_code = 'Date of birth follows date of service'
                elif row[3] == '61':
                    eligible_code = 'Date of death precedes date of service'
                elif row[3] == '62':
                    eligible_code = 'Date of service not within allowable inquiry period'
                elif row[3] == '63':
                    eligible_code = 'date of service in future'
                elif row[3] == '71':
                    eligible_code = 'Patient date of birth does not match patient in database'
                elif row[3] == '72':
                    eligible_code = 'Invalid/missing subscriber/insured ID'
                elif row[3] == '73':
                    eligible_code = 'Invalid/missing subscriber/insured name'
                elif row[3] == '74':
                    eligible_code = 'Invalid/missing subscriber/insured gender code'
                elif row[3] == '75':
                    eligible_code = 'Subscriber/insurer not found'
                elif row[3] == '76':
                    eligible_code = 'Duplicate subscriber/insurer ID number'
                elif row[3] == '78':
                    eligible_code = 'Subscriber/insured not in group/plan identified'
                else:
                    eligible_code = "AAA"


            elif row[0] == 'NM1' and row[1] == 'IL':
                patient_lastname, patient_firstname, CIN = row[3], row[4], row[9]

            elif row[0] == 'DMG' and row[1] == 'D8':
                dob = row[2]
                dob = datetime.strptime(dob, '%Y%m%d').date()
                gender = row[3]

            elif row[0] == 'DTP' and row[1] == '472':
                service_date = row[3]
                service_date = datetime.strptime(service_date, '%Y%m%d').date()

            elif (row[0] == 'EB' and row[1] == 'U' and row[2] == 'IND' and row[3] == '30') or \
                    (row[0] == 'EB' and row[1] == '1' and row[2] =='IND' and row[3] == '30') :
                eligible_code = row[5]

            elif row[0] == 'EB' and row[1] == '6':
                eligible_code = 'Inactive'

            elif row[0] == 'NM1' and row[1]=='Y2':
                payer_name = row[3]
                plan_code = row[9]

                address_row1 = receipt_df.ix[l+1, 1]  #N3
                address_row2 = receipt_df.ix[l+2, 1]  #N4
                contact_row = receipt_df.ix[l+3, 1]   #PER

                address = address_row1[1] + " " + address_row2[1] + " " + address_row2[2] + " " + address_row2[3]
                # contact_name = contact_row[2]
                contact_tel = contact_row[4]

            elif row[0] == 'NM1' and row[1] == 'P4':
                other_payer_name.append(row[3])
                try:
                    other_payer_group_number.append(row[9])
                except:
                    other_payer_group_number.append("")

                N3_row = receipt_df.ix[l+1, 1]
                N4_row = receipt_df.ix[l+2, 1]
                PER_row = receipt_df.ix[l+3, 1]

                if N3_row[0] == "N3" and N4_row[0] == "N4":
                    other_payer_address.append(N3_row[1] + " " + N4_row[1] + " " + N4_row[2] + " " + N4_row[3])
                    if PER_row[0] == 'PER' and PER_row[1] == 'IC':
                        other_payer_tel_tmp = PER_row[4]
                    else:
                        other_payer_tel_tmp = "0"

                    other_payer_telephone.append(other_payer_tel_tmp)
                else:
                    other_payer_address.append("")
                    other_payer_telephone.append("")

            elif row[0] == 'REF' and row[1] == '18':
                other_payer_policy_number = row[2]

            elif row[0] == 'EB' and row[1] == '1' and row[2] == 'IND':
                covered_service_codes.append(str(row[3]))

            if other_payer_name.__len__() == 1:
                other_payer_name1 = other_payer_name[0]
                other_payer_address1 = other_payer_address[0]
                other_payer_telephone1 = other_payer_telephone[0]
                other_payer_group_number1 = other_payer_group_number[0]

            if other_payer_name.__len__() == 2:
                other_payer_name2 = other_payer_name[1]
                other_payer_address2 = other_payer_address[1]
                other_payer_telephone2 = other_payer_telephone[1]
                other_payer_group_number2 = other_payer_group_number[1]

            temp_dict[str(invoice_num)] = {'Invoice number': invoice_num,
                                           'Patient lastname': patient_lastname,
                                           'Patient firstname': patient_firstname,
                                           'Patient DOB': dob,
                                           'Patient gender': gender,
                                           'CIN': CIN,
                                           'Service date': service_date,
                                           'Eligible': eligible_code,
                                           'Payer name': payer_name,
                                           'Payer address': address,
                                           # 'Contact name': contact_name,
                                           'Contact Tel.': contact_tel,
                                           'Plan code': plan_code,
                                           'Covered Codes': str(covered_service_codes),

                                           'Other Payer1 name': other_payer_name1,
                                           'Other Payer1 address': other_payer_address1,
                                           'Other Payer1 tel.': other_payer_telephone1,
                                           'Other Payer1 group number': other_payer_group_number1,
                                           'Other Payer2 name': other_payer_name2,
                                           'Other Payer2 address': other_payer_address2,
                                           'Other Payer2 tel.': other_payer_telephone2,
                                           'Other Payer2 group number': other_payer_group_number2,
                                           'Other Payer policy number': other_payer_policy_number,
                                           }

            if row[0] == 'SE':   # section ends
                # print(temp_dict)

                ifPlanCodeInDB = SQ.IfplancodeInDB(table='PlanCodeLib', plancode=plan_code)
                if ifPlanCodeInDB:
                    eligible_result = "OKAY"
                elif eligible_code in ["ELIGIBLE PCP", "Community Coverage w/CBLTC", "EP - Family Planning and Non Emerg Trans Only", 'MA Eligible',
                 'Eligible Only Outpatient Care', 'Community Coverage No LTC', 'Outpatient Coverage w/ CBLTC'] and ifPlanCodeInDB == False:
                    eligible_result = "OKAY"
                else:
                    eligible_result = 'PENDING'

                temp_dict[str(invoice_num)] = {'Invoice number': invoice_num,
                                               'Eligibility Result': eligible_result,
                                               'Patient lastname': patient_lastname,
                                               'Patient firstname': patient_firstname,
                                               'Patient DOB': dob,
                                               'Patient gender': gender,
                                               'CIN': CIN,
                                               'Service date': service_date,
                                               'Eligible': eligible_code,
                                               'Payer name': payer_name,
                                               'Payer address': address,
                                               # 'Contact name': contact_name,
                                               'Contact Tel.': contact_tel,
                                               'Plan code': plan_code,
                                               'Covered Codes': str(covered_service_codes),

                                               'Other Payer1 name': other_payer_name1,
                                               'Other Payer1 address': other_payer_address1,
                                               'Other Payer1 tel.': other_payer_telephone1,
                                               'Other Payer1 group number': other_payer_group_number1,
                                               'Other Payer2 name': other_payer_name2,
                                               'Other Payer2 address': other_payer_address2,
                                               'Other Payer2 tel.': other_payer_telephone2,
                                               'Other Payer2 group number': other_payer_group_number2,
                                               'Other Payer policy number': other_payer_policy_number,
                                               }

                result_dict.update(temp_dict)
                # Reset var.
                invoice_num = ""
                patient_lastname = ""
                patient_firstname = ""
                CIN = ""
                dob = ""
                gender = ""
                service_date = ""
                eligible_code = ""
                payer_name = ""
                plan_code = ""
                address = ""
                # contact_name = ""
                contact_tel = ""
                other_payer_name1 = ""
                other_payer_address1 = ""
                other_payer_telephone1 = ""
                other_payer_group_number1 = ""
                other_payer_name2 = ""
                other_payer_address2 = ""
                other_payer_telephone2 = ""
                other_payer_group_number2 = ""

                other_payer_name = []
                other_payer_address = []
                other_payer_policy_number = ""
                eligible_result = ""
                other_payer_telephone = []
                other_payer_group_number = []

                covered_service_codes = []
                temp_dict = {}

        result_dict.pop("")
        result_df = pd.DataFrame(result_dict)
        result_df = result_df.transpose()
        result_df = result_df[['Invoice number', 'Eligibility Result', 'Service date', 'Patient firstname', 'Patient lastname', 'Plan code', 'Eligible', 'CIN', 'Covered Codes', 'Patient DOB', 'Patient gender',
                               'Payer name', 'Payer address', 'Contact Tel.', 'Other Payer1 name', 'Other Payer1 address', 'Other Payer1 tel.', 'Other Payer1 group number',
                               'Other Payer2 name', 'Other Payer2 address', 'Other Payer2 tel.', 'Other Payer2 group number', 'Other Payer policy number']]

        file_name_271 = str(datetime.today().date()) + str(datetime.now().time().strftime("%H%M%S"))

        result_df.to_excel(os.path.join(file_saving_path, '271-' + file_name_271 + '.xlsx'), index=False)
        return result_df

    @staticmethod
    def generate_276(receipt837_file, edi837_data, lined_file=True):
        df_837 = pd.read_csv(edi837_data) if edi837_data[-1] == 'v' else pd.read_excel(edi837_data)

        if lined_file==False:    # for raw receipt data
            receipt_df = pd.read_csv(receipt837_file, delimiter="~", header=None,)
            receipt_df = receipt_df.transpose()
            receipt_df.columns = ['line']
            receipt_df['line'] = receipt_df['line'].dropna()
        else:          # already split into lines
            receipt_df = pd.read_csv(receipt837_file)
            receipt_df.columns = ['line']

        receipt_df = receipt_df.dropna()
        receipt_df['line_sep'] = receipt_df['line'].apply(lambda x: x.split("*"))

        result_dict = {}
        temp_dict = {}

        invoice_num = ""
        patient_lastname = ""
        patient_firstname = ""
        CIN = ""
        claim_number = ""
        service_date = ""

        for l in range(receipt_df.__len__()):
            row = receipt_df.ix[l, 1]

            if row[0] == 'BHT':
                invoice_num = row[3]

            elif row[0] == 'NM1' and row[1] == 'QC' and row[2] == '1':
                patient_lastname = row[3]
                patient_firstname = row[4]
                CIN = row[9]

            elif row[0] == 'REF' and row[1] == '1K':
                claim_number = row[2]

            elif row[0] == 'DTP' and row[1] == '472':
                service_date = row[3]

            temp_dict[str(invoice_num)] = {
                'INVOICE NUMBER': invoice_num,
                'CLIENT LAST NAME': patient_lastname,
                'CLIENT FIRST NAME': patient_firstname,
                'MEDICAID ID NUMBER': CIN,
                'CLAIM CONTROL NUMBER': claim_number,
                'SVC DATE': service_date,
            }

            if row[0] == 'SE':   # section ends
                idx_837 = df_837.loc[df_837['invoice number'] == int(invoice_num)].index.tolist()

                if idx_837.__len__() == 0:
                    patient_dob = "00001225"
                    patient_gender = "M"
                else:
                    patient_dob = df_837.ix[idx_837[0], 'patient dob']
                    patient_dob = datetime.strptime(str(patient_dob), '%m/%d/%Y').strftime('%Y%m%d')
                    patient_gender = df_837.ix[idx_837[0], 'patient gender']

                temp_dict[str(invoice_num)] = {
                    'INVOICE NUMBER': invoice_num,
                    'CLIENT LAST NAME': patient_lastname,
                    'CLIENT FIRST NAME': patient_firstname,
                    'MEDICAID ID NUMBER': CIN,
                    'CLAIM CONTROL NUMBER': claim_number,
                    'SVC DATE': service_date,
                    'GENDER': patient_gender,
                    'DOB': patient_dob
                }

                result_dict.update(temp_dict)
                invoice_num = ""
                patient_lastname = ""
                patient_firstname = ""
                CIN = ""
                claim_number = ""
                service_date = ""

        result_dict.pop("")
        result_df = pd.DataFrame(result_dict)
        result_df = result_df.transpose()
        result_df = result_df[['INVOICE NUMBER', 'DOB', 'GENDER', 'CLIENT LAST NAME',
                               'CLIENT FIRST NAME', 'MEDICAID ID NUMBER', 'CLAIM CONTROL NUMBER', 'SVC DATE']]
        result_df.to_excel(os.path.join(file_saving_path,'276-data-' + str(datetime.today().date()) + '.xlsx'), index=False)
        # 276 data is ready and output an excel file to show 276 data
        # To generate edi 276 file now

        edi = EDI276(result_df)
        stream_276_data = edi.ISA_IEA()
        filename = edi.file_name
        Process_Method().write_txt(stream_276_data, os.path.join(file_saving_path, filename))
        return

    @staticmethod
    def process_276_receipt(receipt_file, lined_file=True):
        if lined_file==False:    # for raw receipt data
            receipt_df = pd.read_csv(receipt_file, delimiter="~", header=None,)
            receipt_df = receipt_df.transpose()
            receipt_df.columns = ['line']
            receipt_df['line'] = receipt_df['line'].dropna().apply(lambda x: x)
        else:          # already split into lines
            receipt_df = pd.read_csv(receipt_file)
            receipt_df.columns = ['line']

        receipt_df = receipt_df.dropna()
        receipt_df['line_sep'] = receipt_df['line'].apply(lambda x: x.split("*"))

        result_dict = {}
        temp_dict = {}

        invoice_num = ""
        patient_lastname = ""
        patient_firstname = ""
        CIN = ""
        total_expected_amt = ""
        total_paid_amt = ""
        claim_ctrl_num = ""
        service_date = ""
        encode_expected_list = [0, 0, 0, 0, 0, 0]
        encode_paid_list = [0, 0, 0, 0, 0, 0]
        error_codes = []
        result = ''

        for l in range(receipt_df.__len__()):
            row = receipt_df.ix[l, 1]

            if row[0] == 'BHT':
                invoice_num = row[3]

            elif row[0] == 'NM1' and row[1] == 'IL' and row[2] == '1':
                patient_lastname = row[3]
                patient_firstname = row[4]
                CIN = row[9]

            elif row[0] == 'TRN' and row[1] == '2':
                next_row = receipt_df.ix[l+1, 1]
                total_expected_amt = float(next_row[4])
                total_paid_amt = float(next_row[5])

                if abs(total_expected_amt - total_paid_amt) <= 0.02:
                    result = 'Paid'
                elif total_paid_amt == 0:
                    result = 'Denied'
                else:
                    result = 'Partial Paid'

            elif row[0] == 'REF' and row[1] == '1K':
                claim_ctrl_num = row[2]

            elif row[0] == 'DTP' and row[1] == '472' and row[2] == 'RD8':
                service_date = row[3]

            elif row[0] == 'SVC' and row[1] == 'HC:A0100':
                encode_expected_list[0] = float(row[2])
                encode_paid_list[0] = float(row[3])
                if encode_expected_list[0] != encode_paid_list[0]:
                    error_codes.append('A0100')

            elif row[0] == 'SVC' and row[1] == 'HC:A0100:TN':
                encode_expected_list[1] = float(row[2])
                encode_paid_list[1] = float(row[3])
                if encode_expected_list[1] != encode_paid_list[1]:
                    error_codes.append('A0100TN')

            elif row[0] == 'SVC' and row[1] == 'HC:S0215':
                encode_expected_list[2] = float(row[2])
                encode_paid_list[2] = float(row[3])
                if abs(encode_expected_list[2] - encode_paid_list[2]) > 0.02:
                    error_codes.append('S0215')

            elif row[0] == 'SVC' and row[1] == 'HC:S0215:TN':
                encode_expected_list[3] = float(row[2])
                encode_paid_list[3] = float(row[3])
                if encode_expected_list[3] != encode_paid_list[3]:
                    error_codes.append('S0215TN')

            elif row[0] == 'SVC' and row[1] == 'HC:A0100:SC':
                encode_expected_list[4] = float(row[2])
                encode_paid_list[4] = float(row[3])
                if encode_expected_list[4] != encode_paid_list[4]:
                    error_codes.append('A0100SC')

            elif row[0] == 'SVC' and row[1] == 'HC:A0170:CG':
                encode_expected_list[5] = float(row[2])
                encode_paid_list[5] = float(row[3])
                if encode_expected_list[5] != encode_paid_list[5]:
                    error_codes.append('A0170CG')

            if row[0] == 'SE': # section ends

                if error_codes.__len__() != 0:
                    result = result + "(Wrong Codes: {0})".format(",".join(error_codes))

                temp_dict[str(invoice_num)] = {
                    'Invoice Number': invoice_num,
                    'Patient Lastname': patient_lastname,
                    'Patient Firstname': patient_firstname,
                    'CIN': CIN,
                    'Claim Ctrl Number': claim_ctrl_num,
                    'Total Expected Amt': total_expected_amt,
                    'Total Paid Amt': total_paid_amt,
                    'Service Date': service_date,
                    'Encoded Expected Amt': encode_expected_list,
                    'Encoded Paid Amt': encode_paid_list,
                    'Result': result,
                }

                result_dict.update(temp_dict)

                #reset var.
                invoice_num = ""
                patient_lastname = ""
                patient_firstname = ""
                CIN = ""
                total_expected_amt = ""
                total_paid_amt = ""
                claim_ctrl_num = ""
                service_date = ""
                encode_expected_list = [0, 0, 0, 0, 0, 0]
                encode_paid_list = [0, 0, 0, 0, 0, 0]
                error_codes = []
                result = ''

        # result_dict.pop("")
        result_df = pd.DataFrame(result_dict)
        result_df = result_df.transpose()
        result_df = result_df[['Invoice Number', 'Result', 'Total Expected Amt', 'Total Paid Amt', 'Encoded Expected Amt', 'Encoded Paid Amt',
                               'Patient Lastname', 'Patient Firstname', 'CIN', 'Claim Ctrl Number', 'Service Date']]

        file_name_276277 = str(datetime.today().date()) + str(datetime.now().time().strftime("%H%M%S"))

        result_df.to_excel(os.path.join(file_saving_path, '276-277-' + file_name_276277 + '.xlsx'), index=False)

    @staticmethod
    def generate_270(mas_raw_file):
        raw_df = pd.read_table(mas_raw_file) if mas_raw_file[-1] == 't' else pd.read_csv(mas_raw_file)
        raw_df['Pick-up Address'] = raw_df['Pick-up Address'].apply(lambda x: Process_Method.clean_address(x))
        raw_df['Drop-off Address'] = raw_df['Drop-off Address'].apply(lambda x: Process_Method.clean_address(x))

        result_df = pd.DataFrame()
        unique_invoice = raw_df["Invoice Number"].unique().tolist()
        invoice_number_list = []
        service_name = []
        service_npi = []
        client_lastname = []
        client_firstname = []
        CIN = []
        gender = []
        dob = []
        service_date = []

        for invoice_number in unique_invoice:
            idx = raw_df.loc[
                (raw_df['Invoice Number'] == invoice_number) & (raw_df['Record Type'] == 'Leg')].index.tolist()

            invoice_number_list.append(invoice_number)
            service_name.append(raw_df.ix[idx[0], 'Medical Provider'].upper())
            service_npi.append(int(raw_df.ix[idx[0], 'Ordering Provider ID']))
            client_lastname.append(raw_df.ix[idx[0], 'Last Name'].upper())
            client_firstname.append(raw_df.ix[idx[0], 'First Name'].upper())
            CIN.append(raw_df.ix[idx[0], 'CIN'])
            gender.append(raw_df.ix[idx[0], 'Gender'])
            dob.append(raw_df.ix[idx[0], 'Birthdate'])
            service_date.append(raw_df.ix[idx[0], 'Service Starts'])

        result_df['INVOICE NUMBER'] = invoice_number_list
        result_df['SVC NAME'] = service_name
        result_df['SVC NPI'] = service_npi
        result_df['CLIENT LAST NAME'] = client_lastname
        result_df['CLIENT FIRST NAME'] = client_firstname
        result_df['MEDICAID ID NUMBER'] = CIN
        result_df['GENDER'] = gender
        result_df['DOB'] = dob
        result_df['SVC DATE'] = service_date
        result_df.to_excel(os.path.join(file_saving_path,'270-data-' + str(datetime.today().date()) + str(datetime.now().time().strftime('%H%M%S')) + '.xlsx'), index=False)
        # 270 data is ready and output an excel file to show 270 data
        # to generate edi 270 file now

        edi = EDI270(result_df)
        stream_270_data = edi.ISA_IEA()
        filename = edi.file_name

        Process_Method().write_txt(stream_270_data, os.path.join(file_saving_path, filename))
        # Process_Method().transfer2lines(filename)

        return

    @staticmethod
    def generate_837(data):
        edi = EDI837P(data)
        stream_837data = edi.ISA_IEA()
        filename = edi.file_name + '.txt'
        Process_Method().write_txt(stream_837data, os.path.join(file_saving_path, filename))

        return

    @staticmethod
    def mas_signoff(mas_raw_data, total_job):   # use MAS raw data and total jobs file to generate Processed MAS file and sign-off
        processed_mas_df = Process_MAS(mas_raw_data).add_codes(tofile=True)
        # this step will output an excel and return a pandas dataframe for further processing

        S = SignoffAndCompare()
        S.sign_off(processed_mas_df, total_job, tofile=True)


class window(QMainWindow):

    def __init__(self):
        super(window, self).__init__()
        self.setGeometry(500, 500, 480, 280)
        self.setWindowTitle('EDI')

        plancode_lib = QAction('PlanCode Lib', self)
        plancode_lib.triggered.connect(self.open_plancode_subwindow)

        process_txt = QAction('Process .TXT', self)
        process_txt.triggered.connect(self.process_TXT)

        add_base = QAction('New Base', self)
        add_base.triggered.connect(self.AddNewBase)

        add_driver = QAction('New Driver', self)
        add_driver.triggered.connect(self.AddNewDriver)

        mainMenu = self.menuBar()
        tool = mainMenu.addMenu('&Tools')
        tool.addAction(plancode_lib)
        tool.addAction(process_txt)
        tool.addAction(add_base)
        tool.addAction(add_driver)

        SQ = mysqlite('EDI.db')
        base_df = pd.read_sql("SELECT * FROM AllBases", con=SQ.conn)
        dict_base_df = base_df.to_dict('list')
        self.only_basenames = dict_base_df['BaseName']

        self.home()

    def home(self):
        btn837 = QPushButton('837', self)
        btn837.clicked.connect(self.open_837_subwindow)
        btn837.resize(100, 40)
        btn837.move(50, 20)

        nameLabel1 = QLabel('Base:', self)
        self.base_combobox = QComboBox(self)
        self.base_combobox.addItem('')
        for base_name in self.only_basenames:
            self.base_combobox.addItem(base_name)
        # self.base_combobox.addItem('Clean Air Base')
        nameLabel1.move(280, 52)
        self.base_combobox.move(320, 20)
        self.base_combobox.resize(100, 100)
        self.base_combobox.activated[str].connect(self.Select_base_and_save_info)

        btn270_271 = QPushButton('270/271', self)
        btn270_271.clicked.connect(self.open_270_271_subwindow)
        btn270_271.resize(100, 40)
        btn270_271.move(50, 80)

        btn276_277 = QPushButton('276/277', self)
        btn276_277.clicked.connect(self.open_276_277_subwindow)
        btn276_277.resize(100, 40)
        btn276_277.move(50, 140)

        btnMAS = QPushButton('MAS Billing', self)
        btnMAS.clicked.connect(self.open_MAS_subwindow)
        btnMAS.resize(100, 40)
        btnMAS.move(50, 200)

        btnQuit = QPushButton('Close', self)
        btnQuit.clicked.connect(self.close_application)
        btnQuit.resize(100, 40)
        btnQuit.move(380, 230)

        self.show()

    def close_application(self):
        choice = QMessageBox.question(self, 'Message',
                                     "Are you sure to quit?", QMessageBox.Yes |
                                     QMessageBox.No, QMessageBox.No)

        if choice == QMessageBox.Yes:
            sys.exit()
        else:
            pass

    def open_837_subwindow(self):
        if not info_locker.base_info:
            QMessageBox.about(self, 'Message', 'Select Base first!')
        else:
            self.new_837_window = subwindow_837()
            self.new_837_window.show()

    def open_270_271_subwindow(self):
        if datetime.today().date().day == 1:
            # reset 271 manual check lib
            SQ = mysqlite('EDI.db')
            SQ.delete_all_manually271Lib('ManuallyCheck271')
            QMessageBox.about(self, 'Message', 'Manual Check Lib has been reset!')

        self.new_270_271_window = subwindow_270_271()
        self.new_270_271_window.show()

    def open_276_277_subwindow(self):
        if not info_locker.base_info:
            QMessageBox.about(self, 'Message', 'Select Base first!')
        else:
            self.new_276_277_window = subwindow_276_277()
            self.new_276_277_window.show()

    def open_MAS_subwindow(self):
        if not info_locker.base_info:
            QMessageBox.about(self, 'Message', 'Select Base first!')
        else:
            self.new_mas_window = subwindow_MAS()
            self.new_mas_window.show()

    def open_plancode_subwindow(self):
        self.new_plancode_window = subwindow_plancode()
        self.new_plancode_window.show()

    def process_TXT(self):
        self.new_processTXT_window = subwindow_processTXT()
        self.new_processTXT_window.show()

    def AddNewBase(self):
        self.addnewbase = subwindow_addbase()
        self.addnewbase.show()

    def Select_base_and_save_info(self, text):
        # print(text)
        SQ = mysqlite('EDI.db')
        base_df = pd.read_sql("SELECT * FROM AllBases WHERE BaseName='{0}'".format(text), con=SQ.conn)
        dict_base_df = base_df.to_dict('records')
        info_locker.base_info = dict_base_df[0] if dict_base_df else None

        driver_df = pd.read_sql("SELECT * FROM driver_info WHERE Base='{0}'".format(text), con=SQ.conn)
        driver_df.set_index(['Fleet'], inplace=True)
        dict_driver_df = driver_df.to_dict('index')
        info_locker.driver_information = dict_driver_df if dict_driver_df else None

    def AddNewDriver(self):
        self.addnewdriver = subwindow_addDriver()
        self.addnewdriver.show()


class subwindow_837(QMainWindow):
    class My837TabWidget(QTabWidget):

        def __init__(self, parent):
            super(QWidget, self).__init__(parent)
            self.layout = QGridLayout(self)
            self.file_name1 = None
            self.tabs = QTabWidget()
            self.tab1 = QWidget()

            self.tabs.addTab(self.tab1, '-*837*-')

            self.mytab1()

            self.layout.addWidget(self.tabs)
            self.setLayout(self.layout)

        def mytab1(self):
            self.tab1.layout = QGridLayout(self)
            nameLabel1 = QLabel('Data for 837:')
            self.textbox1 = QLineEdit()
            btnSelect1 = QPushButton('...')
            btnSelect1.clicked.connect(self.select_file1)
            btnRun1 = QPushButton('Run')
            btnRun1.clicked.connect(self.generate_837)
            # btnQuit1 = QPushButton('Quit')
            # btnQuit1.clicked.connect(self.close_application)

            self.tab1.layout.addWidget(nameLabel1, 0, 0)
            self.tab1.layout.addWidget(self.textbox1, 0, 1)
            self.tab1.layout.addWidget(btnSelect1, 0, 2)
            self.tab1.layout.addWidget(btnRun1, 0, 3)
            # self.tab1.layout.addWidget(btnQuit1, 3, 0)
            self.tab1.setLayout(self.tab1.layout)

        def select_file1(self):
            self.file_name1, _ = QFileDialog.getOpenFileName(self, 'Open File', options=QFileDialog.DontUseNativeDialog)
            self.textbox1.setText(self.file_name1)

        def close_application(self):
            choice = QMessageBox.question(self, 'Message',
                                          "Are you sure to quit?", QMessageBox.Yes |
                                          QMessageBox.No, QMessageBox.No)

            if choice == QMessageBox.Yes:
                sys.exit()
            else:
                pass

        def generate_837(self):
            choice = QMessageBox.question(self, 'Message', 'Are you sure to generate 837P?',
                                          QMessageBox.Yes | QMessageBox.No, QMessageBox.No)

            if choice == QMessageBox.Yes:

                if not self.file_name1:
                    QMessageBox.about(self, 'Message', 'Error!')

                else:
                    if not info_locker.base_info:
                        QMessageBox.about(self, 'Message', 'Select Base first!')

                    else:
                        P = Process_Method()
                        P.generate_837(self.file_name1)

                        QMessageBox.about(self, 'Message', 'File generated successfully!')
            else:
                pass

    def __init__(self):
        super(subwindow_837, self).__init__()
        self.setGeometry(600, 600, 600, 380)
        self.setWindowTitle('837')
        # pprint(info_locker.driver_information)
        self.home()

    def home(self):
        self.table_widget = self.My837TabWidget(self)
        self.setCentralWidget(self.table_widget)


class subwindow_270_271(QMainWindow):
    class My270_271TabWidget(QTabWidget):

        def __init__(self, parent):
            super(QWidget, self).__init__(parent)
            self.layout = QGridLayout(self)

            self.file_name2_1 = None
            self.file_name3_1 = None
            self.file_name6_1 = None
            self.file_name6_2 = None
            self.file_name6_3 = None
            self.df = pd.DataFrame()

            self.tabs = QTabWidget()
            self.tab2 = QWidget()
            self.tab3 = QWidget()

            self.tabs.addTab(self.tab2, '-*270*-')
            self.tabs.addTab(self.tab3, '-*271*-')

            self.mytab2()
            self.mytab3()

            self.layout.addWidget(self.tabs)
            self.setLayout(self.layout)

        def mytab2(self):
            self.tab2.layout = QGridLayout(self)
            nameLabel1 = QLabel('MAS Raw File:')
            self.textboxTab2_1 = QLineEdit()
            btnSelectTab2_1 = QPushButton('...')
            btnSelectTab2_1.clicked.connect(self.select_fileTab2_1)
            btnRunTab2_1 = QPushButton('Run')
            btnRunTab2_1.clicked.connect(self.generate_270)
            btnQuit2 = QPushButton('Quit')
            btnQuit2.clicked.connect(self.close_application)

            self.tab2.layout.addWidget(nameLabel1, 0, 0)
            self.tab2.layout.addWidget(self.textboxTab2_1, 0, 1)
            self.tab2.layout.addWidget(btnSelectTab2_1, 0, 2)
            self.tab2.layout.addWidget(btnRunTab2_1, 0, 3)
            # self.tab2.layout.addWidget(btnQuit2, 3, 0)
            self.tab2.setLayout(self.tab2.layout)

        def mytab3(self):
            self.tab3.layout = QGridLayout(self)
            nameLabel1 = QLabel('271 File:')
            self.textboxTab3_1 = QLineEdit()
            btnSelectTab3_1 = QPushButton('...')
            btnSelectTab3_1.clicked.connect(self.select_fileTab3_1)
            btnRunTab3 = QPushButton('Run')
            btnRunTab3.clicked.connect(self.process_271)
            btnQuit3 = QPushButton('Quit')
            btnManualCheck = QPushButton('Manual Check')
            btnManualCheck.clicked.connect(self.showManually271Lib)
            # btnQuit3.clicked.connect(self.close_application)
            btnShowData = QPushButton('Display Data')
            btnShowData.clicked.connect(self.switchToShow)

            # btnShowPendingData = QPushButton('Display the Pendings')
            # btnShowPendingData.clicked.connect(self.switchToShowPending)

            self.tab3.layout.addWidget(nameLabel1, 0, 0)
            self.tab3.layout.addWidget(self.textboxTab3_1, 0, 1)
            self.tab3.layout.addWidget(btnSelectTab3_1, 0, 2)
            self.tab3.layout.addWidget(btnShowData, 1, 3)
            self.tab3.layout.addWidget(btnManualCheck, 1, 0)
            self.tab3.layout.addWidget(btnRunTab3, 0, 3)
            # self.tab3.layout.addWidget(btnQuit3, 3, 0)
            self.tab3.setLayout(self.tab3.layout)

        def select_fileTab2_1(self):
            self.file_name2_1, _ = QFileDialog.getOpenFileName(self, 'Open File',
                                                               options=QFileDialog.DontUseNativeDialog)
            self.textboxTab2_1.setText(self.file_name2_1)

        def select_fileTab3_1(self):
            self.file_name3_1, _ = QFileDialog.getOpenFileName(self, 'Select File',
                                                               options=QFileDialog.DontUseNativeDialog)
            self.textboxTab3_1.setText(self.file_name3_1)

        def close_application(self):
            choice = QMessageBox.question(self, 'Message',
                                          "Are you sure to quit?", QMessageBox.Yes |
                                          QMessageBox.No, QMessageBox.No)

            if choice == QMessageBox.Yes:
                sys.exit()
            else:
                pass

        def generate_270(self):
            choice = QMessageBox.question(self, 'Message', 'Are you sure to generate EDI 270?',
                                          QMessageBox.Yes | QMessageBox.No, QMessageBox.No)

            if choice == QMessageBox.Yes:
                if not self.file_name2_1:
                    QMessageBox.about(self, 'Message', 'Error!')

                else:

                    P = Process_Method()
                    P.generate_270(self.file_name2_1)
                    QMessageBox.about(self, 'Message', 'File generated successfully!')

            else:
                pass

        def create_table(self, data):
            self.tableWidget = QTableWidget(self)
            countRow = data.__len__()
            countCol = data.shape[1]
            headers = data.columns.tolist()
            self.tableWidget.setRowCount(countRow)
            self.tableWidget.setColumnCount(countCol)
            for r in range(countRow):
                for c in range(countCol):
                    self.tableWidget.setItem(r, c, QTableWidgetItem(str(data.ix[r, c])))

            self.tableWidget.setHorizontalHeaderLabels(headers)
            self.tableWidget.resizeColumnsToContents()
            self.tableWidget.resizeRowsToContents()
            self.tableWidget.move(0, 0)

            self.layout.addWidget(self.tableWidget)
            self.tab4.setLayout(self.tab4.layout)
            self.show()

        def switchToShow(self):

            if self.df.__len__() == 0:
                QMessageBox.about(self, 'Message', 'Process Data First!')
            else:
                # self.create_table(self.df)
                self.sub_win = EDI270data_subwindow(self.df)
                self.sub_win.show()

        def process_271(self):
            choice = QMessageBox.question(self, 'Message', 'Are you sure to process EDI 271?',
                                          QMessageBox.Yes | QMessageBox.No, QMessageBox.No)

            if choice == QMessageBox.Yes:
                if not self.file_name3_1:
                    QMessageBox.about(self, 'Message', 'Error!')
                else:
                    P = Process_Method()
                    SQ = mysqlite('EDI.db')
                    self.df = P.process_270_receipt(self.file_name3_1, lined_file=False)
                    SQ.upsert271(table='Eligibility271', data=self.df)
                    self.pendingFromdf = self.df.loc[self.df['Eligibility Result'] == 'PENDING']
                    self.pendingFromdf_cinList = self.pendingFromdf['CIN'].tolist()

                    self.manual_df = SQ.generate_excel_from_manually271Lib(table='ManuallyCheck271', tofile=False)
                    self.eligibleFrom_manual_df_cinList = self.manual_df.loc[self.manual_df['Eligible'] == 'Eligible', 'CIN'].tolist()

                    if self.eligibleFrom_manual_df_cinList.__len__() != 0:
                        for pendingCIN in self.pendingFromdf_cinList:
                            if pendingCIN in self.eligibleFrom_manual_df_cinList:
                                pendingCIN_idx = self.pendingFromdf.loc[self.pendingFromdf['CIN'] == pendingCIN].index.tolist()
                                self.pendingFromdf = self.pendingFromdf.drop(index=pendingCIN_idx)

                    self.pendingFromdf.to_excel(os.path.join(file_saving_path, '271-Not eligible Trips' + str(datetime.today().date()) + "-" + str(datetime.now().time().strftime("%H%M%S")) + '.xlsx'), index=False)
                    QMessageBox.about(self, 'Message', 'File Processed Successfully!')

            else:
                pass

        def showManually271Lib(self):
            self.subwindow_manuallyChecking = subwindow_manually271Lib()
            self.subwindow_manuallyChecking.show()

    def __init__(self):
        super(subwindow_270_271, self).__init__()
        self.setGeometry(600, 600, 600, 380)
        self.setWindowTitle('270/271')
        self.home()

    def home(self):
        self.table_widget = self.My270_271TabWidget(self)
        self.setCentralWidget(self.table_widget)


class subwindow_manually271Lib(QMainWindow):
    class MyManualCheckTabWidget(QTabWidget):

        def __init__(self, parent):
            super(QWidget, self).__init__(parent)
            self.layout = QGridLayout(self)

            self.tabs = QTabWidget()
            self.tab1 = QWidget()
            self.tabs.addTab(self.tab1, 'Manual Checking Lib')

            self.mytab1()

            self.layout.addWidget(self.tabs)
            self.setLayout(self.layout)

        def mytab1(self):
            self.tab1.layout = QGridLayout(self)
            nameLabel1 = QLabel('- Add/Update -')
            nameLabel2 = QLabel('Eligible:')
            nameLabel3 = QLabel('First Name:')
            nameLabel4 = QLabel('Last Name:')
            nameLabel5 = QLabel('CIN:')
            nameLabel6 = QLabel('Description:')
            nameLabel7 = QLabel('- Delete -')
            nameLabel8 = QLabel('CIN:')

            self.textboxTab1_1 = QComboBox(self)
            self.textboxTab1_1.addItem('Not Eligible')
            self.textboxTab1_1.addItem('Eligible')
            self.textboxTab1_2 = QLineEdit()
            self.textboxTab1_3 = QLineEdit()
            self.textboxTab1_4 = QLineEdit()
            self.textboxTab1_5 = QLineEdit()
            self.textboxTab1_6 = QLineEdit()

            btnAddUpdate = QPushButton('Add/Update')
            btnAddUpdate.clicked.connect(self.AddUpdateManualLib)

            btnOutputExcel = QPushButton('Get Excel')
            btnOutputExcel.clicked.connect(self.OutputExcel)

            btnDelete = QPushButton('Delete')
            btnDelete.clicked.connect(self.DeleteManualLibByCIN)

            btnShowData = QPushButton('Display Lib')
            btnShowData.clicked.connect(self.ShowManualCheckLib)

            self.tab1.layout.addWidget(nameLabel1, 0, 1)
            self.tab1.layout.addWidget(nameLabel2, 1, 0)
            self.tab1.layout.addWidget(nameLabel3, 2, 0)
            self.tab1.layout.addWidget(nameLabel4, 3, 0)
            self.tab1.layout.addWidget(nameLabel5, 4, 0)
            self.tab1.layout.addWidget(nameLabel6, 5, 0)
            self.tab1.layout.addWidget(self.textboxTab1_1, 1, 1)
            self.tab1.layout.addWidget(self.textboxTab1_2, 2, 1)
            self.tab1.layout.addWidget(self.textboxTab1_3, 3, 1)
            self.tab1.layout.addWidget(self.textboxTab1_4, 4, 1)
            self.tab1.layout.addWidget(self.textboxTab1_5, 5, 1)
            self.tab1.layout.addWidget(btnOutputExcel, 6, 0)
            self.tab1.layout.addWidget(btnAddUpdate, 6, 1)

            self.tab1.layout.addWidget(nameLabel7, 0, 3)
            self.tab1.layout.addWidget(nameLabel8, 2, 2)
            self.tab1.layout.addWidget(self.textboxTab1_6, 2, 3)
            self.tab1.layout.addWidget(btnDelete, 6, 3)
            self.tab1.layout.addWidget(btnShowData, 7, 0)
            self.tab1.setLayout(self.tab1.layout)

        def AddUpdateManualLib(self):
            SQ = mysqlite('EDI.db')
            eligible = str(self.textboxTab1_1.currentText())
            fn = self.textboxTab1_2.text()
            ln = self.textboxTab1_3.text()
            cin = self.textboxTab1_4.text()
            description = self.textboxTab1_5.text()

            SQ.manuallyUpsert271Lib(table='ManuallyCheck271', eligible=eligible, patient_ln=ln, patient_fn=fn, cin=cin, description=description)
            QMessageBox.about(self, 'Message', 'Client with CIN {0} has been added now!'.format(cin))
            self.textboxTab1_2.setText("")
            self.textboxTab1_3.setText("")
            self.textboxTab1_4.setText("")
            self.textboxTab1_5.setText("")

        def DeleteManualLibByCIN(self):
            SQ = mysqlite('EDI.db')
            cin = self.textboxTab1_6.text()
            SQ.delete_manually271Lib(table='ManuallyCheck271', cin=cin)
            QMessageBox.about(self, 'Message', 'Client with CIN {0} has been removed now!'.format(cin))

        def OutputExcel(self):
            SQ = mysqlite('EDI.db')
            choice = QMessageBox.question(self, 'Message', 'Are you sure to output file?',
                                          QMessageBox.Yes | QMessageBox.No, QMessageBox.No)
            if choice == QMessageBox.Yes:
                SQ.generate_excel_from_manually271Lib(table='ManuallyCheck271')
            else:
                pass
            QMessageBox.about(self, 'Message', 'File generated successfully!')

        def ShowManualCheckLib(self):
            self.show_manualcheck_lib = ShowManualCheck_subwindow()
            self.show_manualcheck_lib.show()

    def __init__(self):
        super(subwindow_manually271Lib, self).__init__()
        self.setGeometry(600, 600, 600, 380)
        self.setWindowTitle('Manually Checking Lib')
        self.home()

    def home(self):
        self.table_widget = self.MyManualCheckTabWidget(self)
        self.setCentralWidget(self.table_widget)


class ShowManualCheck_subwindow(QMainWindow):
    def __init__(self):
        super(ShowManualCheck_subwindow, self).__init__()
        self.setGeometry(600, 600, 750, 400)

        self.setWindowTitle('EDI GUI')
        self.home()

    def home(self):
        SQ = mysqlite('EDI.db')
        data = SQ.generate_excel_from_manually271Lib(table='ManuallyCheck271', tofile=False)

        self.tableWidget = QTableWidget(self)
        countRow = data.__len__()
        countCol = data.shape[1]
        headers = data.columns.tolist()
        self.tableWidget.setRowCount(countRow)
        self.tableWidget.setColumnCount(countCol)
        for r in range(countRow):
            for c in range(countCol):
                self.tableWidget.setItem(r, c, QTableWidgetItem(str(data.ix[r, c])))

        self.tableWidget.setHorizontalHeaderLabels(headers)
        self.tableWidget.resizeColumnsToContents()
        self.tableWidget.resizeRowsToContents()
        self.tableWidget.move(0, 0)
        self.tableWidget.resize(2560, 1440)


class EDI270data_subwindow(QMainWindow):

    def __init__(self, data):
        super(EDI270data_subwindow, self).__init__()
        self.data = data
        self.setGeometry(600, 600, 1000, 500)
        self.setWindowTitle('EDI GUI')
        self.home()

    def home(self):
        self.tableWidget = QTableWidget(self)
        countRow = self.data.__len__()
        countCol = self.data.shape[1]
        headers = self.data.columns.tolist()
        self.tableWidget.setRowCount(countRow)
        self.tableWidget.setColumnCount(countCol)
        for r in range(countRow):
            for c in range(countCol):
                self.tableWidget.setItem(r, c, QTableWidgetItem(str(self.data.ix[r, c])))

        self.tableWidget.setHorizontalHeaderLabels(headers)
        self.tableWidget.resizeColumnsToContents()
        self.tableWidget.resizeRowsToContents()
        self.tableWidget.move(0, 0)
        self.tableWidget.resize(1000, 500)


class EDI271_pending_subwindow(QMainWindow):
    def __init__(self):
        super(EDI271_pending_subwindow, self).__init__()
        self.data = pd.read_sql("SELECT * FROM UNKNOWN271", con=mysqlite('EDI.db').conn)
        self.setGeometry(600, 600, 1000, 500)
        self.setWindowTitle('EDI GUI')
        self.home()

    def home(self):
        self.tableWidget = QTableWidget(self)
        countRow = self.data.__len__()
        countCol = self.data.shape[1]
        headers = self.data.columns.tolist()
        self.tableWidget.setRowCount(countRow)
        self.tableWidget.setColumnCount(countCol)
        for r in range(countRow):
            for c in range(countCol):
                self.tableWidget.setItem(r, c, QTableWidgetItem(str(self.data.ix[r, c])))

        self.tableWidget.doubleClicked.connect(self.clickAndChange)

        self.tableWidget.setHorizontalHeaderLabels(headers)
        self.tableWidget.resizeColumnsToContents()
        self.tableWidget.resizeRowsToContents()
        self.tableWidget.move(0, 0)
        self.tableWidget.resize(1000, 500)

    def clickAndChange(self, cell):
        row_idx = cell.row()
        col_idx = cell.column()
        print(self.tableWidget.item(row_idx, col_idx).text())


class subwindow_276_277(QMainWindow):
    class My276_277TabWidget(QTabWidget):

        def __init__(self, parent):
            super(QWidget, self).__init__(parent)
            self.layout = QGridLayout(self)

            self.file_name1 = None
            self.file_name2_1 = None
            self.file_name3_1 = None
            self.file_name6_1 = None
            self.file_name6_2 = None
            self.file_name6_3 = None
            self.df = pd.DataFrame()

            self.tabs = QTabWidget()

            self.tab6 = QWidget()
            self.tab7 = QWidget()

            self.tabs.addTab(self.tab6, '-*276*-')
            self.tabs.addTab(self.tab7, '-*277*-')

            self.mytab6()
            self.mytab7()

            self.layout.addWidget(self.tabs)
            self.setLayout(self.layout)

        def mytab6(self):
            self.tab6.layout = QGridLayout(self)
            nameLabel1 = QLabel('Data for 837:')
            self.textboxTab6_1 = QLineEdit()
            btnSelectTab6_1 = QPushButton('...')
            btnSelectTab6_1.clicked.connect(self.select_fileTab6_1)
            nameLabel2 = QLabel('837 Response 277:')
            self.textboxTab6_2 = QLineEdit()
            btnSelectTab6_2 = QPushButton('...')
            btnSelectTab6_2.clicked.connect(self.select_fileTab6_2)
            btnRun6 = QPushButton('Run')
            btnRun6.clicked.connect(self.generate_276)

            self.tab6.layout.addWidget(nameLabel1, 0, 0)
            self.tab6.layout.addWidget(self.textboxTab6_1, 0, 1)
            self.tab6.layout.addWidget(btnSelectTab6_1, 0, 2)
            self.tab6.layout.addWidget(nameLabel2, 1, 0)
            self.tab6.layout.addWidget(self.textboxTab6_2, 1, 1)
            self.tab6.layout.addWidget(btnSelectTab6_2, 1, 2)
            self.tab6.layout.addWidget(btnRun6, 2, 2)
            self.tab6.setLayout(self.tab6.layout)

        def mytab7(self):
            self.tab7.layout = QGridLayout(self)
            self.nameLabel3 = QLabel("Process 276's 277:")
            self.textboxTab6_3 = QLineEdit()
            self.btnSelectTab6_3 = QPushButton('...')
            self.btnSelectTab6_3.clicked.connect(self.select_fileTab6_3)
            self.btnProcess = QPushButton('Process')
            self.btnProcess.clicked.connect(self.process276277)

            self.tab7.layout.addWidget(self.nameLabel3, 0, 0)
            self.tab7.layout.addWidget(self.textboxTab6_3, 0, 1)
            self.tab7.layout.addWidget(self.btnSelectTab6_3, 0, 2)
            self.tab7.layout.addWidget(self.btnProcess, 0, 3)
            self.tab7.setLayout(self.tab7.layout)

        def select_fileTab6_1(self):
            self.file_name6_1, _ = QFileDialog.getOpenFileName(self, 'Select File',
                                                               options=QFileDialog.DontUseNativeDialog)
            self.textboxTab6_1.setText(self.file_name6_1)

        def select_fileTab6_2(self):
            self.file_name6_2, _ = QFileDialog.getOpenFileName(self, 'Select File',
                                                               options=QFileDialog.DontUseNativeDialog)
            self.textboxTab6_2.setText(self.file_name6_2)

        def select_fileTab6_3(self):
            self.file_name6_3, _ = QFileDialog.getOpenFileName(self, 'Select File',
                                                               options=QFileDialog.DontUseNativeDialog)
            self.textboxTab6_3.setText(self.file_name6_3)

        def close_application(self):
            choice = QMessageBox.question(self, 'Message',
                                          "Are you sure to quit?", QMessageBox.Yes |
                                          QMessageBox.No, QMessageBox.No)

            if choice == QMessageBox.Yes:
                sys.exit()
            else:
                pass

        def generate_276(self):
            choice = QMessageBox.question(self, 'Message', 'Are you sure to generate EDI 276?',
                                          QMessageBox.Yes | QMessageBox.No, QMessageBox.No)

            if choice == QMessageBox.Yes:
                if not self.file_name6_1 or not self.file_name6_2:
                    QMessageBox.about(self, 'Message', 'Error!')

                else:
                    if not info_locker.base_info:
                        QMessageBox.about(self, 'Message', 'Select Base first!')
                    else:
                        P = Process_Method()
                        data_for276 = P.generate_276(receipt837_file=self.file_name6_2, edi837_data=self.file_name6_1,
                                                           lined_file=False)
                        QMessageBox.about(self, 'Message', 'File generated successfully!')
            else:
                pass

        def process276277(self):
            choice = QMessageBox.question(self, 'Message', "Are you sure to process EDI 276's 277?",
                                          QMessageBox.Yes | QMessageBox.No, QMessageBox.No)

            if choice == QMessageBox.Yes:
                if not self.file_name6_3:
                    QMessageBox.about(self, 'Message', 'Error!')

                else:
                    P = Process_Method()
                    P.process_276_receipt(self.file_name6_3, lined_file=False)
                    QMessageBox.about(self, 'Message', 'File Processed Successfully!')

            else:
                pass

    def __init__(self):
        super(subwindow_276_277, self).__init__()
        self.setGeometry(600, 600, 600, 380)
        self.setWindowTitle('276/277')
        self.home()

    def home(self):
        self.table_widget = self.My276_277TabWidget(self)
        self.setCentralWidget(self.table_widget)


class subwindow_MAS(QMainWindow):
    class MyMASTabWidget(QTabWidget):
        def __init__(self, parent):
            super(QWidget, self).__init__(parent)
            self.layout = QGridLayout(self)
            # self.layout.setSpacing(2)

            self.tabs = QTabWidget()
            self.tab1 = QWidget()
            self.tab2 = QWidget()
            self.tab3 = QWidget()
            self.tab4 = QWidget()

            self.tabs.addTab(self.tab2, 'MAS Sign Off')
            self.tabs.addTab(self.tab3, 'Sign-off and PA Roster')
            self.tabs.addTab(self.tab4, 'Check Payment')

            self.bool_to837_file = False
            self.filenameTab3_3 = None
            self.ifcollect270 = False
            self.only270 = False

            self.mytab2()
            self.mytab3()
            self.mytab4()

            ############### add tabs to Widget
            self.layout.addWidget(self.tabs)
            self.setLayout(self.layout)

            ################ tab 1  Process MAS from RAW DATA

        def mytab2(self):
            self.tab2.layout = QGridLayout(self)
            nameLabel1 = QLabel('MAS Raw Data:')
            self.textbox1 = QLineEdit()
            btnSelect1 = QPushButton('...')
            btnSelect1.clicked.connect(self.select_file1)
            nameLabel2 = QLabel('Total Jobs:')
            self.textbox2 = QLineEdit()
            btnSelect2 = QPushButton('...')
            btnSelect2.clicked.connect(self.select_file2)
            btnRun2 = QPushButton('Run')
            btnRun2.clicked.connect(self.mas_sign_off)
            btnQuit2 = QPushButton('Quit')
            btnQuit2.clicked.connect(self.close_application)

            # self.checkboxTab2 = QCheckBox('Also generate 837 file?', self)
            # self.checkboxTab2.stateChanged.connect(self.clickbox)

            self.tab2.layout.addWidget(nameLabel1, 0, 0)
            self.tab2.layout.addWidget(self.textbox1, 0, 1)
            self.tab2.layout.addWidget(btnSelect1, 0, 2)
            self.tab2.layout.addWidget(nameLabel2, 1, 0)
            self.tab2.layout.addWidget(self.textbox2, 1, 1)
            self.tab2.layout.addWidget(btnSelect2, 1, 2)
            self.tab2.layout.addWidget(btnRun2, 2, 2)

            # self.tab2.layout.addWidget(btnQuit2, 3, 0)
            self.tab2.setLayout(self.tab2.layout)

            ################### tab3 SIGN OFF compares with PA roast ################

        def mytab3(self):

            self.tab3.layout = QGridLayout(self)
            nameLabelTab3_1 = QLabel('Sign-off:')
            self.textboxTab3_1 = QLineEdit()
            btnSelectTab3_1 = QPushButton('...')
            btnSelectTab3_1.clicked.connect(self.select_fileTab3_1)
            nameLabelTab3_2 = QLabel('PA Roster:')
            self.textboxTab3_2 = QLineEdit()
            btnSelectTab3_2 = QPushButton('...')
            btnSelectTab3_2.clicked.connect(self.select_fileTab3_2)
            btnRunTab3 = QPushButton('Run')
            btnRunTab3.clicked.connect(self.signoff_W_PA)
            btnQuitTab3 = QPushButton('Quit')
            btnQuitTab3.clicked.connect(self.close_application)

            self.checkboxTab3 = QCheckBox('Collect data for 837?', self)
            self.checkboxTab3.stateChanged.connect(self.clickbox)

            self.nameLabelTab3_3 = QLabel('Processed MAS:')
            self.textboxTab3_3 = QLineEdit()
            self.btnSelectTab3_3 = QPushButton('...')
            self.btnSelectTab3_3.clicked.connect(self.select_fileTab3_3)

            self.nameLabelTab3_3.hide()
            self.textboxTab3_3.hide()
            self.btnSelectTab3_3.hide()

            self.tab3.layout.addWidget(nameLabelTab3_1, 0, 0)
            self.tab3.layout.addWidget(self.textboxTab3_1, 0, 1)
            self.tab3.layout.addWidget(btnSelectTab3_1, 0, 2)
            self.tab3.layout.addWidget(nameLabelTab3_2, 1, 0)
            self.tab3.layout.addWidget(self.textboxTab3_2, 1, 1)
            self.tab3.layout.addWidget(btnSelectTab3_2, 1, 2)

            self.tab3.layout.addWidget(self.nameLabelTab3_3, 2, 0)
            self.tab3.layout.addWidget(self.textboxTab3_3, 2, 1)
            self.tab3.layout.addWidget(self.btnSelectTab3_3, 2, 2)

            self.tab3.layout.addWidget(btnRunTab3, 3, 2)
            self.tab3.layout.addWidget(self.checkboxTab3, 3, 0)
            # self.tab3.layout.addWidget(btnQuitTab3, 4, 0)
            self.tab3.setLayout(self.tab3.layout)

        def mytab4(self):
            self.tab4.layout = QGridLayout(self)
            nameLabelTab4_1 = QLabel('MAS Correction:')
            self.textboxTab4_1 = QLineEdit()
            btnSelectTab4_1 = QPushButton('...')
            btnSelectTab4_1.clicked.connect(self.select_fileTab4_1)
            nameLabelTab4_2 = QLabel('Payment:')
            self.textboxTab4_2 = QLineEdit()
            btnSelectTab4_2 = QPushButton('...')
            btnSelectTab4_2.clicked.connect(self.select_fileTab4_2)
            btnRunTab4 = QPushButton('Run')
            btnRunTab4.clicked.connect(self.compare_after_payment)
            btnQuitTab4 = QPushButton('Quit')
            btnQuitTab4.clicked.connect(self.close_application)

            self.tab4.layout.addWidget(nameLabelTab4_1, 0, 0)
            self.tab4.layout.addWidget(self.textboxTab4_1, 0, 1)
            self.tab4.layout.addWidget(btnSelectTab4_1, 0, 2)
            self.tab4.layout.addWidget(nameLabelTab4_2, 1, 0)
            self.tab4.layout.addWidget(self.textboxTab4_2, 1, 1)
            self.tab4.layout.addWidget(btnSelectTab4_2, 1, 2)
            self.tab4.layout.addWidget(btnRunTab4, 3, 2)
            # self.tab4.layout.addWidget(btnQuitTab4, 4, 0)
            self.tab4.setLayout(self.tab4.layout)

        def select_file(self):
            self.file_name, _ = QFileDialog.getOpenFileName(self, 'Select File',
                                                            options=QFileDialog.DontUseNativeDialog)
            self.textbox.setText(self.file_name)

        def select_file1(self):
            self.file_name1, _ = QFileDialog.getOpenFileName(self, 'Select File',
                                                             options=QFileDialog.DontUseNativeDialog)
            self.textbox1.setText(self.file_name1)

        def select_file2(self):
            self.file_name2, _ = QFileDialog.getOpenFileName(self, 'Select File',
                                                             options=QFileDialog.DontUseNativeDialog)
            self.textbox2.setText(self.file_name2)

        def close_application(self):
            choice = QMessageBox.question(self, 'Message',
                                          "Are you sure to quit?", QMessageBox.Yes |
                                          QMessageBox.No, QMessageBox.No)

            if choice == QMessageBox.Yes:
                sys.exit()
            else:
                pass

        def SwitchToCollect270(self, state):
            if state == Qt.Checked:
                self.ifcollect270 = True
            else:
                self.ifcollect270 = False

        def OnlyCollect270(self, state):
            if state == Qt.Checked:
                self.only270 = True
            else:
                self.only270 = False

        def clickbox(self, state):
            if state == Qt.Checked:
                self.nameLabelTab3_3.show()
                self.textboxTab3_3.show()
                self.btnSelectTab3_3.show()
                self.bool_to837_file = True

            else:
                self.nameLabelTab3_3.hide()
                self.textboxTab3_3.hide()
                self.btnSelectTab3_3.hide()
                self.bool_to837_file = False

        def mas_raw_process(self):
            choice = QMessageBox.question(self, 'Message', 'Are you sure to process raw data?',
                                          QMessageBox.Yes | QMessageBox.No, QMessageBox.No)

            if choice == QMessageBox.Yes:
                p = Process_MAS(self.file_name)

                if self.only270 == True:
                    p.generate_270_data(tofile=True)

                elif self.ifcollect270 == True:
                    p.generate_270_data(tofile=True)
                    p.add_codes(tofile=True)

                sleep(0.5)
                QMessageBox.about(self, 'Message', 'File generated successfully!')

            else:
                pass

        def mas_sign_off(self):
            choice = QMessageBox.question(self, 'Message', 'Are you sure to process raw data?',
                                          QMessageBox.Yes | QMessageBox.No, QMessageBox.No)

            if choice == QMessageBox.Yes:
                Process_Method().mas_signoff(mas_raw_data=self.file_name1, total_job=self.file_name2)
                QMessageBox.about(self, 'Message', 'File generated successfully!')

            else:
                pass

        def select_fileTab3_1(self):
            self.filenameTab3_1, _ = QFileDialog.getOpenFileName(self, 'Select File',
                                                                 options=QFileDialog.DontUseNativeDialog)
            self.textboxTab3_1.setText(self.filenameTab3_1)

        def select_fileTab3_2(self):
            self.filenameTab3_2, _ = QFileDialog.getOpenFileName(self, 'Select File',
                                                                 options=QFileDialog.DontUseNativeDialog)
            self.textboxTab3_2.setText(self.filenameTab3_2)

        def select_fileTab3_3(self):
            self.filenameTab3_3, _ = QFileDialog.getOpenFileName(self, 'Select File',
                                                                 options=QFileDialog.DontUseNativeDialog)
            self.textboxTab3_3.setText(self.filenameTab3_3)

        def select_fileTab4_1(self):
            self.filenameTab4_1, _ = QFileDialog.getOpenFileName(self, 'Select File',
                                                                 options=QFileDialog.DontUseNativeDialog)
            self.textboxTab4_1.setText(self.filenameTab4_1)

        def select_fileTab4_2(self):
            self.filenameTab4_2, _ = QFileDialog.getOpenFileName(self, 'Select File',
                                                                 options=QFileDialog.DontUseNativeDialog)
            self.textboxTab4_2.setText(self.filenameTab4_2)

        def compare_after_payment(self):
            choice = QMessageBox.question(self, 'Message', 'Are you sure to use payment file to compare?',
                                          QMessageBox.Yes | QMessageBox.No, QMessageBox.No)

            if choice == QMessageBox.Yes:
                S = SignoffAndCompare()
                S.new_compare_after_payment(signoff_compare_PA_file=self.filenameTab4_1,
                                            payment_raw_file=self.filenameTab4_2)
                sleep(0.5)
                QMessageBox.about(self, 'Message', 'File generated successfully!')

            else:
                pass

        def signoff_W_PA(self):
            choice = QMessageBox.question(self, 'Message', 'Are you sure to compare two files?',
                                          QMessageBox.Yes | QMessageBox.No, QMessageBox.No)

            if choice == QMessageBox.Yes:
                S = SignoffAndCompare()
                S.compare_signoff_PA(self.filenameTab3_1, self.filenameTab3_2, tofile=True, to837=self.bool_to837_file,
                                     mas_2=self.filenameTab3_3)
                sleep(0.5)
                QMessageBox.about(self, 'Message', 'File Generated Successfully!')

            else:
                pass

    def __init__(self):
        super(subwindow_MAS, self).__init__()
        self.setGeometry(600, 600, 600, 380)
        self.setWindowTitle('MAS Billing')
        self.home()

    def home(self):
        self.table_widget = self.MyMASTabWidget(self)
        self.setCentralWidget(self.table_widget)


class subwindow_plancode(QMainWindow):
    class MyPlanCodeTabWidget(QTabWidget):

        def __init__(self, parent):
            super(QWidget, self).__init__(parent)
            self.layout = QGridLayout(self)

            self.tabs = QTabWidget()
            self.tab4 = QWidget()
            self.tabs.addTab(self.tab4, 'PlanCode Lib')

            self.mytab4()

            self.layout.addWidget(self.tabs)
            self.setLayout(self.layout)

        def mytab4(self):
            # plan code library
            self.tab4.layout = QGridLayout(self)
            nameLabel1 = QLabel('- Add/Update -')
            nameLabel2 = QLabel('Plan Code:')
            nameLabel3 = QLabel('Provider Name:')
            nameLabel4 = QLabel('Tel.:')
            nameLabel5 = QLabel('Plan Type:')
            nameLabel6 = QLabel('- Delete -')
            nameLabel7 = QLabel('Plan Code:')

            self.textboxTab4_2 = QLineEdit()
            self.textboxTab4_3 = QLineEdit()
            self.textboxTab4_4 = QLineEdit()
            self.textboxTab4_5 = QLineEdit()
            self.textboxTab4_7 = QLineEdit()

            btnAddUpdate = QPushButton('Add/Update')
            btnAddUpdate.clicked.connect(self.AddUpdatePlancode)
            btnDelete = QPushButton('Delete')
            btnDelete.clicked.connect(self.DeletePlancode)
            btnShowLib = QPushButton('Show Lib')
            btnShowLib.clicked.connect(self.ShowLib)
            btnQuit4 = QPushButton('Quit')
            btnQuit4.clicked.connect(self.close_application)

            self.tab4.layout.addWidget(nameLabel1, 0, 1)
            self.tab4.layout.addWidget(nameLabel2, 1, 0)
            self.tab4.layout.addWidget(self.textboxTab4_2, 1, 1)
            self.tab4.layout.addWidget(nameLabel3, 2, 0)
            self.tab4.layout.addWidget(self.textboxTab4_3, 2, 1)
            self.tab4.layout.addWidget(nameLabel4, 3, 0)
            self.tab4.layout.addWidget(self.textboxTab4_4, 3, 1)
            self.tab4.layout.addWidget(nameLabel5, 4, 0)
            self.tab4.layout.addWidget(self.textboxTab4_5, 4, 1)
            self.tab4.layout.addWidget(nameLabel6, 0, 3)
            self.tab4.layout.addWidget(nameLabel7, 2, 2)
            self.tab4.layout.addWidget(self.textboxTab4_7, 2, 3)
            self.tab4.layout.addWidget(btnAddUpdate, 5, 1)
            self.tab4.layout.addWidget(btnDelete, 5, 3)
            # self.tab4.layout.addWidget(btnQuit4, 7, 0)
            self.tab4.layout.addWidget(btnShowLib, 6, 0)
            self.tab4.setLayout(self.tab4.layout)

        def close_application(self):
            choice = QMessageBox.question(self, 'Message',
                                          "Are you sure to quit?", QMessageBox.Yes |
                                          QMessageBox.No, QMessageBox.No)

            if choice == QMessageBox.Yes:
                sys.exit()
            else:
                pass

        def AddUpdatePlancode(self):
            SQ = mysqlite('EDI.db')
            plancode = self.textboxTab4_2.text()
            providername = self.textboxTab4_3.text()
            tel = self.textboxTab4_4.text()
            plantype = self.textboxTab4_5.text()
            SQ.upsert_271_plancodes(table='PlanCodeLib', plancode=plancode, providername=providername,
                                    tel=tel, plantype=plantype)

            QMessageBox.about(self, 'Message', 'Plan code {0} has been added now!'.format(plancode))

        def DeletePlancode(self):
            SQ = mysqlite('EDI.db')
            del_plancode = self.textboxTab4_7.text()
            SQ.delete_271_plancodes(table='PlanCodeLib', plancode=del_plancode)
            QMessageBox.about(self, 'Message', 'Plan code {0} has been deleted now!'.format(del_plancode))

        def ShowLib(self):
            # SQ = mysqlite('EDI.db')
            # data = SQ.get_data_from_271_plancode('PlanCodeLib')
            #
            # self.tableWidget = QTableWidget(self)
            # countRow = data.__len__()
            # countCol = data.shape[1]
            # headers = data.columns.tolist()
            # self.tableWidget.setRowCount(countRow)
            # self.tableWidget.setColumnCount(countCol)
            # for r in range(countRow):
            #     for c in range(countCol):
            #         self.tableWidget.setItem(r, c, QTableWidgetItem(str(data.ix[r, c])))
            #
            # self.tableWidget.setHorizontalHeaderLabels(headers)
            # self.tableWidget.resizeColumnsToContents()
            # self.tableWidget.resizeRowsToContents()
            # self.tableWidget.move(0, 0)
            #
            # self.layout.addWidget(self.tableWidget)
            # self.setLayout(self.layout)
            # self.show()
            self.second_win = ShowLib_subwindow()
            self.second_win.show()

    def __init__(self):
        super(subwindow_plancode, self).__init__()
        self.setGeometry(600, 600, 600, 380)
        self.setWindowTitle('PlanCode Lib')
        self.home()

    def home(self):
        self.table_widget = self.MyPlanCodeTabWidget(self)
        self.setCentralWidget(self.table_widget)


class ShowLib_subwindow(QMainWindow):

    def __init__(self):
        super(ShowLib_subwindow, self).__init__()
        self.setGeometry(600, 600, 750, 400)
        self.setWindowTitle('EDI GUI')
        self.home()

    def home(self):
        SQ = mysqlite('EDI.db')
        data = SQ.get_data_from_271_plancode('PlanCodeLib')

        self.tableWidget = QTableWidget(self)
        countRow = data.__len__()
        countCol = data.shape[1]
        headers = data.columns.tolist()
        self.tableWidget.setRowCount(countRow)
        self.tableWidget.setColumnCount(countCol)
        for r in range(countRow):
            for c in range(countCol):
                self.tableWidget.setItem(r, c, QTableWidgetItem(str(data.ix[r, c])))

        self.tableWidget.setHorizontalHeaderLabels(headers)
        self.tableWidget.resizeColumnsToContents()
        self.tableWidget.resizeRowsToContents()
        self.tableWidget.move(0, 0)
        self.tableWidget.resize(750, 400)


class subwindow_processTXT(QMainWindow):
    class MyTXTTabWidget(QTabWidget):

        def __init__(self, parent):
            super(QWidget, self).__init__(parent)
            self.layout = QGridLayout(self)

            self.file_name5_1 = None

            self.df = pd.DataFrame()

            self.tabs = QTabWidget()
            self.tab5 = QWidget()
            self.tabs.addTab(self.tab5, 'Process Method')

            self.mytab5()

            self.layout.addWidget(self.tabs)
            self.setLayout(self.layout)

        def mytab5(self):
            self.tab5.layout = QGridLayout(self)
            nameLabel1 = QLabel('TXT File:')
            self.textboxTab5_1 = QLineEdit()
            btnToLines = QPushButton('To Lines')
            btnToStream = QPushButton('To Stream')
            btnToLines.clicked.connect(self.transfer2lines)
            btnToStream.clicked.connect(self.transfer2stream)
            btnSelectFileTab5_1 = QPushButton('...')
            btnSelectFileTab5_1.clicked.connect(self.select_fileTab5_1)

            self.tab5.layout.addWidget(nameLabel1, 0, 0)
            self.tab5.layout.addWidget(self.textboxTab5_1, 0, 1)
            self.tab5.layout.addWidget(btnSelectFileTab5_1, 0, 2)
            self.tab5.layout.addWidget(btnToLines, 1, 2)
            self.tab5.layout.addWidget(btnToStream, 2, 2)
            self.tab5.setLayout(self.tab5.layout)

        def select_fileTab5_1(self):
            self.file_name5_1, _ = QFileDialog.getOpenFileName(self, 'Select File',
                                                               options=QFileDialog.DontUseNativeDialog)
            self.textboxTab5_1.setText(self.file_name5_1)

        def close_application(self):
            choice = QMessageBox.question(self, 'Message',
                                          "Are you sure to quit?", QMessageBox.Yes |
                                          QMessageBox.No, QMessageBox.No)

            if choice == QMessageBox.Yes:
                sys.exit()
            else:
                pass

        def transfer2lines(self):
            P = Process_Method()
            if not self.file_name5_1:
                QMessageBox.about(self, 'Message', 'Error!')
            else:
                P.transfer2lines(self.file_name5_1)
                QMessageBox.about(self, 'Message', 'File generated successfully!')

        def transfer2stream(self):
            P = Process_Method()
            if not self.file_name5_1:
                QMessageBox.about(self, 'Message', 'Error!')

            else:
                P.transfer2stream(self.file_name5_1)
                QMessageBox.about(self, 'Message', 'File generated successfully!')

    def __init__(self):
        super(subwindow_processTXT, self).__init__()
        self.setGeometry(600, 600, 600, 380)
        self.setWindowTitle('Process TXT File')
        self.home()

    def home(self):
        self.table_widget = self.MyTXTTabWidget(self)
        self.setCentralWidget(self.table_widget)


class subwindow_addbase(QMainWindow):
    class MyAddBaseTabWidget(QTabWidget):

        def __init__(self, parent):
            super(QWidget, self).__init__(parent)
            self.layout = QGridLayout(self)

            self.tabs = QTabWidget()
            self.tab1 = QWidget()
            self.tabs.addTab(self.tab1, 'Add Base')

            self.mytab1()

            self.layout.addWidget(self.tabs)
            self.setLayout(self.layout)

        def mytab1(self):
            self.tab1.layout = QGridLayout(self)
            nameLabel0 = QLabel('Add Base:')
            nameLabel1 = QLabel('Name:')
            nameLabel2 = QLabel('Address:')
            nameLabel3 = QLabel('City:')
            nameLabel4 = QLabel('State:')
            nameLabel5 = QLabel('ZipCode:')
            nameLabel6 = QLabel('ETIN:')
            nameLabel7 = QLabel('NPI:')
            nameLabel8 = QLabel('Medicaid Provider#:')
            nameLabel9 = QLabel('TaxID:')
            nameLabel10 = QLabel('Contact Name:')
            nameLabel11 = QLabel('Contact Tel.:')
            nameLabel12 = QLabel('Location Code:')

            nameLabel13 = QLabel('- Delete Base -')
            nameLabel14 = QLabel('NPI:')

            self.textbox1 = QLineEdit()
            self.textbox2 = QLineEdit()
            self.textbox3 = QLineEdit()
            self.state_combobox = QComboBox(self)
            self.state_combobox.addItem("")
            self.state_combobox.addItem("NY")
            self.textbox5 = QLineEdit()
            self.textbox6 = QLineEdit()
            self.textbox7 = QLineEdit()
            self.textbox8 = QLineEdit()
            self.textbox9 = QLineEdit()
            self.textbox10 = QLineEdit()
            self.textbox11 = QLineEdit()
            self.textbox12 = QLineEdit()
            self.textbox13 = QLineEdit()

            # self.tab1.layout.addWidget(nameLabel0, 0, 0)
            self.tab1.layout.addWidget(nameLabel1, 1, 0)
            self.tab1.layout.addWidget(nameLabel2, 2, 0)
            self.tab1.layout.addWidget(nameLabel3, 3, 0)
            self.tab1.layout.addWidget(nameLabel4, 4, 0)
            self.tab1.layout.addWidget(nameLabel5, 5, 0)
            self.tab1.layout.addWidget(nameLabel6, 6, 0)
            self.tab1.layout.addWidget(nameLabel7, 7, 0)
            self.tab1.layout.addWidget(nameLabel8, 8, 0)
            self.tab1.layout.addWidget(nameLabel9, 9, 0)
            self.tab1.layout.addWidget(nameLabel10, 10, 0)
            self.tab1.layout.addWidget(nameLabel11, 11, 0)
            self.tab1.layout.addWidget(nameLabel12, 12, 0)
            self.tab1.layout.addWidget(nameLabel13, 14, 0)
            self.tab1.layout.addWidget(nameLabel14, 15, 0)

            btnCreateBase = QPushButton('Create')
            btnCancel = QPushButton('Cancel')
            btnCreateBase.clicked.connect(self.createBase2DB)

            btnDelete = QPushButton('Delete')
            btnDelete.clicked.connect(self.deleteBase)

            btnShowBase = QPushButton('Show Base')
            btnShowBase.clicked.connect(self.showbase)

            self.tab1.layout.addWidget(self.textbox1, 1, 1)
            self.tab1.layout.addWidget(self.textbox2, 2, 1)
            self.tab1.layout.addWidget(self.textbox3, 3, 1)
            self.tab1.layout.addWidget(self.state_combobox, 4, 1)
            self.tab1.layout.addWidget(self.textbox5, 5, 1)
            self.tab1.layout.addWidget(self.textbox6, 6, 1)
            self.tab1.layout.addWidget(self.textbox7, 7, 1)
            self.tab1.layout.addWidget(self.textbox8, 8, 1)
            self.tab1.layout.addWidget(self.textbox9, 9, 1)
            self.tab1.layout.addWidget(self.textbox10, 10, 1)
            self.tab1.layout.addWidget(self.textbox11, 11, 1)
            self.tab1.layout.addWidget(self.textbox12, 12, 1)
            self.tab1.layout.addWidget(self.textbox13, 15, 1)

            self.tab1.layout.addWidget(btnCreateBase, 13, 1)
            self.tab1.layout.addWidget(btnShowBase, 13, 0)
            self.tab1.layout.addWidget(btnDelete, 16, 1)

            self.tab1.setLayout(self.tab1.layout)

        def createBase2DB(self):
            basename = self.textbox1.text()
            baseaddress = self.textbox2.text()
            city = self.textbox3.text()
            state = self.state_combobox.currentText()
            zipcode = self.textbox5.text()
            etin = self.textbox6.text()
            npi = self.textbox7.text()
            medicaid_providerNum = self.textbox8.text()
            taxid = self.textbox9.text()
            contactname = self.textbox10.text()
            contacttel = self.textbox11.text()
            locationcode = self.textbox12.text()

            if not basename or not baseaddress or not city or not state or not zipcode or not etin or not npi or not medicaid_providerNum or not taxid or not contactname or not contacttel or not locationcode:
                QMessageBox.about(self, 'Message', 'All fields are required!')
            else:
                SQ = mysqlite('EDI.db')
                SQ.upsert_newbase(table='AllBases', basename=basename, baseaddress=baseaddress, city=city, state=state, zipcode=zipcode, etin=etin, npi=npi,
                                  medicaid_provider_num=medicaid_providerNum, taxid=taxid, contactname=contactname, contactTel=contacttel, locationcode=locationcode)
                QMessageBox.about(self, 'Message', 'New base {0} are added!'.format(basename))

        def deleteBase(self):
            npi = self.textbox13.text()
            SQ = mysqlite('EDI.db')
            SQ.delete_newbase(table='AllBases', npi=npi)
            QMessageBox.about(self, 'Message', 'Base {0} are deleted!'.format(npi))

        def showbase(self):
            self.show_new_base = subwindow_ShowNewBase()
            self.show_new_base.show()

    def __init__(self):
        super(subwindow_addbase, self).__init__()
        self.setGeometry(600, 600, 400, 600)
        self.setWindowTitle('Add A New Base')
        self.home()

    def home(self):
        self.table_widget = self.MyAddBaseTabWidget(self)
        self.setCentralWidget(self.table_widget)


class subwindow_ShowNewBase(QMainWindow):

    def __init__(self):
        super(subwindow_ShowNewBase, self).__init__()
        self.setGeometry(600, 600, 750, 400)
        self.setWindowTitle('EDI GUI')
        self.home()

    def home(self):
        SQ = mysqlite('EDI.db')
        data = SQ.get_data_from_271_plancode('AllBases')

        self.tableWidget = QTableWidget(self)
        countRow = data.__len__()
        countCol = data.shape[1]
        headers = data.columns.tolist()
        self.tableWidget.setRowCount(countRow)
        self.tableWidget.setColumnCount(countCol)
        for r in range(countRow):
            for c in range(countCol):
                self.tableWidget.setItem(r, c, QTableWidgetItem(str(data.ix[r, c])))

        self.tableWidget.setHorizontalHeaderLabels(headers)
        self.tableWidget.resizeColumnsToContents()
        self.tableWidget.resizeRowsToContents()
        self.tableWidget.move(0, 0)
        self.tableWidget.resize(750, 400)


class subwindow_addDriver(QMainWindow):
    class MyAddDriverWidget(QTabWidget):
        def __init__(self, parent):
            super(QWidget, self).__init__(parent)
            self.layout = QGridLayout(self)

            self.tabs = QTabWidget()
            self.tab1 = QWidget()
            self.tabs.addTab(self.tab1, 'Add Driver')

            self.only_basenames = window().only_basenames

            self.mytab1()

            self.layout.addWidget(self.tabs)
            self.setLayout(self.layout)

        def mytab1(self):
            self.tab1.layout = QGridLayout(self)
            nameLabel1 = QLabel('Fleet:')
            nameLabel2 = QLabel('Base:')
            nameLabel3 = QLabel('FirstName:')
            nameLabel4 = QLabel('LastName:')
            nameLabel5 = QLabel('Driver ID:')
            nameLabel6 = QLabel('Vehicle ID:')
            nameLabel7 = QLabel('- Delete Driver -')
            nameLabel8 = QLabel('Fleet:')

            self.textbox1 = QLineEdit()
            self.base_combobox = QComboBox(self)
            for base in self.only_basenames:
                self.base_combobox.addItem(base)
            self.textbox3 = QLineEdit()
            self.textbox4 = QLineEdit()
            self.textbox5 = QLineEdit()
            self.textbox6 = QLineEdit()
            self.textbox8 = QLineEdit()

            btnAddDriver = QPushButton('Add')
            btnAddDriver.clicked.connect(self.add_driver2DB)
            btnDeleteDriver = QPushButton('Delete')
            btnDeleteDriver.clicked.connect(self.deleteDriver)
            btnShowDriver = QPushButton('Show Driver')
            btnShowDriver.clicked.connect(self.showDriver)

            self.tab1.layout.addWidget(nameLabel1, 0, 0)
            self.tab1.layout.addWidget(nameLabel2, 1, 0)
            self.tab1.layout.addWidget(nameLabel3, 2, 0)
            self.tab1.layout.addWidget(nameLabel4, 3, 0)
            self.tab1.layout.addWidget(nameLabel5, 4, 0)
            self.tab1.layout.addWidget(nameLabel6, 5, 0)
            self.tab1.layout.addWidget(btnShowDriver, 6, 0)
            self.tab1.layout.addWidget(nameLabel7, 7, 0)
            self.tab1.layout.addWidget(nameLabel8, 8, 0)
            self.tab1.layout.addWidget(self.textbox1, 0, 1)
            self.tab1.layout.addWidget(self.base_combobox, 1, 1)
            self.tab1.layout.addWidget(self.textbox3, 2, 1)
            self.tab1.layout.addWidget(self.textbox4, 3, 1)
            self.tab1.layout.addWidget(self.textbox5, 4, 1)
            self.tab1.layout.addWidget(self.textbox6, 5, 1)
            self.tab1.layout.addWidget(btnAddDriver, 6, 1)
            self.tab1.layout.addWidget(self.textbox8, 8, 1)
            self.tab1.layout.addWidget(btnDeleteDriver, 9, 1)

            self.tab1.setLayout(self.tab1.layout)

        def add_driver2DB(self):
            fleet = self.textbox1.text()
            base = self.base_combobox.currentText()
            firstname = self.textbox3.text()
            lastname = self.textbox4.text()
            driverid = self.textbox5.text()
            vehicleid = self.textbox6.text()


            if not fleet or not base or not firstname or not lastname or not driverid or not vehicleid:
                QMessageBox.about(self, 'Message', 'All fields are required!')
            else:
                SQ = mysqlite('EDI.db')
                SQ.upsert_newdriver(table='driver_info', fleet=fleet, base=base, firstname=firstname, lastname=lastname, driverid=driverid, vehicleid=vehicleid)
                QMessageBox.about(self, 'Message', 'Driver {0} {1} are added'.format(firstname, lastname))
                self.textbox1.setText("")
                self.textbox3.setText("")
                self.textbox4.setText("")
                self.textbox5.setText("")
                self.textbox6.setText("")

        def deleteDriver(self):
            delete_fleet = self.textbox8.text()
            SQ = mysqlite('EDI.db')
            SQ.delete_driver(table='driver_info', fleet=delete_fleet)
            QMessageBox.about(self, 'Message', 'Driver {0} is deleted!'.format(delete_fleet))

        def showDriver(self):
            self.show_driver = subwindow_ShowDriver()
            self.show_driver.show()

    def __init__(self):
        super(subwindow_addDriver, self).__init__()
        self.setGeometry(600, 600, 380, 450)
        self.setWindowTitle('Add A New Driver')
        self.home()

    def home(self):
        self.table_widget = self.MyAddDriverWidget(self)
        self.setCentralWidget(self.table_widget)


class subwindow_ShowDriver(QMainWindow):

    def __init__(self):
        super(subwindow_ShowDriver, self).__init__()
        self.setGeometry(600, 600, 800, 600)
        self.setWindowTitle('EDI GUI')
        self.home()

    def home(self):
        SQ = mysqlite('EDI.db')
        data = SQ.get_data_from_driver('driver_info')
        self.tableWidget = QTableWidget(self)
        countRow = data.__len__()
        countCol = data.shape[1]
        headers = data.columns.tolist()
        self.tableWidget.setRowCount(countRow)
        self.tableWidget.setColumnCount(countCol)
        for r in range(countRow):
            for c in range(countCol):
                self.tableWidget.setItem(r, c, QTableWidgetItem(str(data.ix[r, c])))

        self.tableWidget.setHorizontalHeaderLabels(headers)
        self.tableWidget.resizeColumnsToContents()
        self.tableWidget.resizeRowsToContents()
        self.tableWidget.move(0, 0)
        self.tableWidget.resize(800, 600)


class MyEDITabWidget(QTabWidget):

    def __init__(self, parent):
        super(QWidget, self).__init__(parent)
        self.layout = QGridLayout(self)

        self.file_name1 = None
        self.file_name2_1 = None
        self.file_name3_1 = None
        self.file_name6_1 = None
        self.file_name6_2 = None
        self.file_name6_3 = None
        self.df = pd.DataFrame()

        self.tabs = QTabWidget()
        self.tab1 = QWidget()
        self.tab2 = QWidget()
        self.tab3 = QWidget()
        self.tab4 = QWidget()
        self.tab5 = QWidget()
        self.tab6 = QWidget()

        self.tabs.addTab(self.tab1, '-*837*-')
        self.tabs.addTab(self.tab2, '-*270*-')
        self.tabs.addTab(self.tab3, '-*271*-')
        self.tabs.addTab(self.tab6, '-*276*-')
        self.tabs.addTab(self.tab4, 'PlanCode Lib')
        self.tabs.addTab(self.tab5, 'Process Method')

        self.mytab1()
        self.mytab2()
        self.mytab3()
        self.mytab4()
        self.mytab5()
        self.mytab6()

        self.layout.addWidget(self.tabs)
        self.setLayout(self.layout)

    def mytab1(self):
        self.tab1.layout = QGridLayout(self)
        nameLabel1 = QLabel('Data for 837:')
        self.textbox1 = QLineEdit()
        btnSelect1 = QPushButton('...')
        btnSelect1.clicked.connect(self.select_file1)
        btnRun1 = QPushButton('Run')
        btnRun1.clicked.connect(self.generate_837)
        btnQuit1 = QPushButton('Quit')
        btnQuit1.clicked.connect(self.close_application)

        self.tab1.layout.addWidget(nameLabel1, 0, 0)
        self.tab1.layout.addWidget(self.textbox1, 0, 1)
        self.tab1.layout.addWidget(btnSelect1, 0, 2)
        self.tab1.layout.addWidget(btnRun1, 0, 3)
        self.tab1.layout.addWidget(btnQuit1, 3, 0)
        self.tab1.setLayout(self.tab1.layout)

    def mytab2(self):
        self.tab2.layout = QGridLayout(self)
        nameLabel1 = QLabel('Data for 270:')
        self.textboxTab2_1 = QLineEdit()
        btnSelectTab2_1 = QPushButton('...')
        btnSelectTab2_1.clicked.connect(self.select_fileTab2_1)
        btnRunTab2_1 = QPushButton('Run')
        btnRunTab2_1.clicked.connect(self.generate_270)
        btnQuit2 = QPushButton('Quit')
        btnQuit2.clicked.connect(self.close_application)

        self.tab2.layout.addWidget(nameLabel1, 0, 0)
        self.tab2.layout.addWidget(self.textboxTab2_1, 0, 1)
        self.tab2.layout.addWidget(btnSelectTab2_1, 0, 2)
        self.tab2.layout.addWidget(btnRunTab2_1, 0, 3)
        self.tab2.layout.addWidget(btnQuit2, 3, 0)
        self.tab2.setLayout(self.tab2.layout)

    def mytab3(self):
        self.tab3.layout = QGridLayout(self)
        nameLabel1 = QLabel('271 File:')
        self.textboxTab3_1 = QLineEdit()
        btnSelectTab3_1 = QPushButton('...')
        btnSelectTab3_1.clicked.connect(self.select_fileTab3_1)
        btnRunTab3 = QPushButton('Run')
        btnRunTab3.clicked.connect(self.process_271)
        btnQuit3 = QPushButton('Quit')
        btnQuit3.clicked.connect(self.close_application)
        btnShowData = QPushButton('Display Data')
        btnShowData.clicked.connect(self.switchToShow)

        self.tab3.layout.addWidget(nameLabel1, 0, 0)
        self.tab3.layout.addWidget(self.textboxTab3_1, 0, 1)
        self.tab3.layout.addWidget(btnSelectTab3_1, 0, 2)
        self.tab3.layout.addWidget(btnShowData, 1, 3)
        self.tab3.layout.addWidget(btnRunTab3, 0, 3)
        self.tab3.layout.addWidget(btnQuit3, 3, 0)
        self.tab3.setLayout(self.tab3.layout)

    def mytab4(self):
        # plan code library
        self.tab4.layout = QGridLayout(self)
        nameLabel1 = QLabel('- Add/Update -')
        nameLabel2 = QLabel('Plan Code:')
        nameLabel3 = QLabel('Provider Name:')
        nameLabel4 = QLabel('Tel.:')
        nameLabel5 = QLabel('Plan Type:')
        nameLabel6 = QLabel('- Delete -')
        nameLabel7 = QLabel('Plan Code:')

        self.textboxTab4_2 = QLineEdit()
        self.textboxTab4_3 = QLineEdit()
        self.textboxTab4_4 = QLineEdit()
        self.textboxTab4_5 = QLineEdit()
        self.textboxTab4_7 = QLineEdit()

        btnAddUpdate = QPushButton('Add/Update')
        btnAddUpdate.clicked.connect(self.AddUpdatePlancode)
        btnDelete = QPushButton('Delete')
        btnDelete.clicked.connect(self.DeletePlancode)
        btnShowLib = QPushButton('Show Lib')
        btnShowLib.clicked.connect(self.ShowLib)
        btnQuit4 = QPushButton('Quit')
        btnQuit4.clicked.connect(self.close_application)

        self.tab4.layout.addWidget(nameLabel1, 0, 1)
        self.tab4.layout.addWidget(nameLabel2, 1, 0)
        self.tab4.layout.addWidget(self.textboxTab4_2, 1, 1)
        self.tab4.layout.addWidget(nameLabel3, 2, 0)
        self.tab4.layout.addWidget(self.textboxTab4_3, 2, 1)
        self.tab4.layout.addWidget(nameLabel4, 3, 0)
        self.tab4.layout.addWidget(self.textboxTab4_4, 3, 1)
        self.tab4.layout.addWidget(nameLabel5, 4, 0)
        self.tab4.layout.addWidget(self.textboxTab4_5, 4, 1)
        self.tab4.layout.addWidget(nameLabel6, 0, 3)
        self.tab4.layout.addWidget(nameLabel7, 2, 2)
        self.tab4.layout.addWidget(self.textboxTab4_7, 2, 3)
        self.tab4.layout.addWidget(btnAddUpdate, 5, 1)
        self.tab4.layout.addWidget(btnDelete, 5, 3)
        self.tab4.layout.addWidget(btnQuit4, 7, 0)
        self.tab4.layout.addWidget(btnShowLib, 6, 0)
        self.tab4.setLayout(self.tab4.layout)

    def mytab5(self):
        self.tab5.layout = QGridLayout(self)
        nameLabel1 = QLabel('File:')
        self.textboxTab5_1 = QLineEdit()
        btnToLines = QPushButton('ToLines')
        btnToStream = QPushButton('ToStream')
        btnToLines.clicked.connect(self.transfer2lines)
        btnToStream.clicked.connect(self.transfer2stream)
        btnSelectFileTab5_1 = QPushButton('...')
        btnSelectFileTab5_1.clicked.connect(self.select_fileTab5_1)

        self.tab5.layout.addWidget(nameLabel1, 0, 0)
        self.tab5.layout.addWidget(self.textboxTab5_1, 0, 1)
        self.tab5.layout.addWidget(btnSelectFileTab5_1, 0, 2)
        self.tab5.layout.addWidget(btnToLines, 1, 2)
        self.tab5.layout.addWidget(btnToStream, 2, 2)
        self.tab5.setLayout(self.tab5.layout)

    def mytab6(self):
        self.tab6.layout = QGridLayout(self)
        nameLabel1 = QLabel('Data for 837:')
        self.textboxTab6_1 = QLineEdit()
        btnSelectTab6_1 = QPushButton('...')
        btnSelectTab6_1.clicked.connect(self.select_fileTab6_1)
        nameLabel2 = QLabel('837 Response 277:')
        self.textboxTab6_2 = QLineEdit()
        btnSelectTab6_2 = QPushButton('...')
        btnSelectTab6_2.clicked.connect(self.select_fileTab6_2)
        btnRun6 = QPushButton('Run')
        btnRun6.clicked.connect(self.generate_276)
        btnquit6 = QPushButton('Quit')
        btnquit6.clicked.connect(self.close_application)

        self.checkboxTab6 = QCheckBox('Process 277?', self)
        self.checkboxTab6.stateChanged.connect(self.checkbox276277)

        self.nameLabel3 = QLabel("Process 276's 277:")
        self.textboxTab6_3 = QLineEdit()
        self.btnSelectTab6_3 = QPushButton('...')
        self.btnSelectTab6_3.clicked.connect(self.select_fileTab6_3)
        self.btnProcess = QPushButton('Process')
        self.btnProcess.clicked.connect(self.process276277)

        self.nameLabel3.hide()
        self.textboxTab6_3.hide()
        self.btnSelectTab6_3.hide()
        self.btnProcess.hide()

        self.tab6.layout.addWidget(nameLabel1, 0, 0)
        self.tab6.layout.addWidget(self.textboxTab6_1, 0 ,1)
        self.tab6.layout.addWidget(btnSelectTab6_1, 0, 2)
        self.tab6.layout.addWidget(nameLabel2, 1, 0)
        self.tab6.layout.addWidget(self.textboxTab6_2, 1, 1)
        self.tab6.layout.addWidget(btnSelectTab6_2, 1, 2)
        self.tab6.layout.addWidget(btnRun6, 2, 2)
        self.tab6.layout.addWidget(self.checkboxTab6, 2, 0)
        self.tab6.layout.addWidget(self.nameLabel3, 3, 0)
        self.tab6.layout.addWidget(self.textboxTab6_3, 3, 1)
        self.tab6.layout.addWidget(self.btnSelectTab6_3, 3, 2)
        self.tab6.layout.addWidget(self.btnProcess, 3, 3)

        self.tab6.layout.addWidget(btnquit6, 4, 0)
        self.tab6.setLayout(self.tab6.layout)

    def select_file1(self):
        self.file_name1, _ = QFileDialog.getOpenFileName(self, 'Open File', options=QFileDialog.DontUseNativeDialog)
        self.textbox1.setText(self.file_name1)

    def select_fileTab2_1(self):
        self.file_name2_1, _ = QFileDialog.getOpenFileName(self, 'Open File', options=QFileDialog.DontUseNativeDialog)
        self.textboxTab2_1.setText(self.file_name2_1)

    def select_fileTab3_1(self):
        self.file_name3_1, _ = QFileDialog.getOpenFileName(self, 'Select File', options=QFileDialog.DontUseNativeDialog)
        self.textboxTab3_1.setText(self.file_name3_1)

    def select_fileTab5_1(self):
        self.file_name5_1, _ = QFileDialog.getOpenFileName(self, 'Select File', options=QFileDialog.DontUseNativeDialog)
        self.textboxTab5_1.setText(self.file_name5_1)

    def select_fileTab6_1(self):
        self.file_name6_1, _ = QFileDialog.getOpenFileName(self, 'Select File', options=QFileDialog.DontUseNativeDialog)
        self.textboxTab6_1.setText(self.file_name6_1)

    def select_fileTab6_2(self):
        self.file_name6_2, _ = QFileDialog.getOpenFileName(self, 'Select File', options=QFileDialog.DontUseNativeDialog)
        self.textboxTab6_2.setText(self.file_name6_2)

    def select_fileTab6_3(self):
        self.file_name6_3, _ = QFileDialog.getOpenFileName(self, 'Select File', options=QFileDialog.DontUseNativeDialog)
        self.textboxTab6_3.setText(self.file_name6_3)

    def close_application(self):
        choice = QMessageBox.question(self, 'Message',
                                      "Are you sure to quit?", QMessageBox.Yes |
                                      QMessageBox.No, QMessageBox.No)

        if choice == QMessageBox.Yes:
            sys.exit()
        else:
            pass

    def generate_837(self):
        choice = QMessageBox.question(self, 'Message', 'Are you sure to generate 837P?',
                                      QMessageBox.Yes | QMessageBox.No, QMessageBox.No)

        if choice == QMessageBox.Yes:

            if not self.file_name1:
                QMessageBox.about(self, 'Message', 'Error!')

            else:
                edi = EDI837P(self.file_name1)
                prue_data_837 = edi.ISA_IEA()
                file_name = '837-' + edi.file_name + '.txt'

                P = Process_Method()
                P.write_txt(prue_data_837, file_name)
                P.transfer2lines(file_name, 'L'+file_name)
                QMessageBox.about(self, 'Message', 'File generated successfully!')
        else:
            pass

    def generate_270(self):
        choice = QMessageBox.question(self, 'Message', 'Are you sure to generate EDI 270?',
                                      QMessageBox.Yes | QMessageBox.No, QMessageBox.No)

        if choice == QMessageBox.Yes:
            if not self.file_name2_1:
                QMessageBox.about(self, 'Message', 'Error!')

            else:
                edi = EDI270(self.file_name2_1)
                prue_data_270 = edi.ISA_IEA()
                file_name = edi.file_name

                P = Process_Method()
                P.write_txt(prue_data_270, file_name)
                P.transfer2lines(file_name)
                QMessageBox.about(self, 'Message', 'File generated successfully!')

        else:
            pass

    def generate_276(self):
        choice = QMessageBox.question(self, 'Message', 'Are you sure to generate EDI 276?',
                                      QMessageBox.Yes | QMessageBox.No, QMessageBox.No)

        if choice == QMessageBox.Yes:
            if not self.file_name6_1 or not self.file_name6_2:
                QMessageBox.about(self, 'Message', 'Error!')

            else:
                P = Process_Method()
                data_for276 = P.ready_for_276_data(receipt837_file=self.file_name6_2, edi837_data=self.file_name6_1, lined_file=False)
                edi = EDI276(data_for276)
                prue_data276 = edi.ISA_IEA()
                file_name = edi.file_name
                P.write_txt(prue_data276, file_name)
                P.transfer2lines(file_name)
                QMessageBox.about(self, 'Message', 'File generated successfully!')
        else:
            pass

    def create_table(self, data):
        self.tableWidget = QTableWidget(self)
        countRow = data.__len__()
        countCol = data.shape[1]
        headers = data.columns.tolist()
        self.tableWidget.setRowCount(countRow)
        self.tableWidget.setColumnCount(countCol)
        for r in range(countRow):
            for c in range(countCol):
                self.tableWidget.setItem(r,c, QTableWidgetItem(str(data.ix[r,c])))

        self.tableWidget.setHorizontalHeaderLabels(headers)
        self.tableWidget.resizeColumnsToContents()
        self.tableWidget.resizeRowsToContents()
        self.tableWidget.move(0, 0)

        self.layout.addWidget(self.tableWidget)
        self.tab4.setLayout(self.tab4.layout)
        self.show()

    def switchToShow(self):

        if self.df.__len__() == 0:
            QMessageBox.about(self, 'Message', 'Process Data First!')
        else:
            # self.create_table(self.df)
            self.sub_win = EDI270data_subwindow(self.df)
            self.sub_win.show()

    def process_271(self):
        choice = QMessageBox.question(self, 'Message', 'Are you sure to process EDI 271?',
                                      QMessageBox.Yes | QMessageBox.No, QMessageBox.No)

        if choice == QMessageBox.Yes:
            if not self.file_name3_1:
                QMessageBox.about(self, 'Message', 'Error!')
            else:
                P = Process_Method()
                SQ = mysqlite('EDI.db')

                self.df = P.process_270_receipt(self.file_name3_1, lined_file=False)
                SQ.upsert271(table='Eligibility271', data=self.df)
                QMessageBox.about(self, 'Message', 'File Processed Successfully!')

        else:
            pass

    def process276277(self):
        choice = QMessageBox.question(self, 'Message', "Are you sure to process EDI 276's 277?",
                                      QMessageBox.Yes | QMessageBox.No, QMessageBox.No)

        if choice == QMessageBox.Yes:
            if not self.file_name6_3:
                QMessageBox.about(self, 'Message', 'Error!')

            else:
                P = Process_Method()
                P.process_276_receipt(self.file_name6_3, lined_file=False)
                QMessageBox.about(self, 'Message', 'File Processed Successfully!')

        else:
            pass

    def checkbox276277(self, state):
        if state == Qt.Checked:
            self.nameLabel3.show()
            self.textboxTab6_3.show()
            self.btnSelectTab6_3.show()
            self.btnProcess.show()

        else:
            self.nameLabel3.hide()
            self.textboxTab6_3.hide()
            self.btnSelectTab6_3.hide()
            self.btnProcess.hide()

    def AddUpdatePlancode(self):
        SQ = mysqlite('EDI.db')
        plancode = self.textboxTab4_2.text()
        providername = self.textboxTab4_3.text()
        tel = self.textboxTab4_4.text()
        plantype = self.textboxTab4_5.text()
        SQ.upsert_271_plancodes(table='PlanCodeLib', plancode=plancode, providername=providername,
                                tel=tel, plantype=plantype)

        QMessageBox.about(self, 'Message', 'Plan code {0} has been added now!'.format(plancode))

    def DeletePlancode(self):
        SQ = mysqlite('EDI.db')
        del_plancode = self.textboxTab4_7.text()
        SQ.delete_271_plancodes(table='PlanCodeLib', plancode=del_plancode)
        QMessageBox.about(self, 'Message', 'Plan code {0} has been deleted now!'.format(del_plancode))

    def ShowLib(self):
        # SQ = mysqlite('EDI.db')
        # data = SQ.get_data_from_271_plancode('PlanCodeLib')
        #
        # self.tableWidget = QTableWidget(self)
        # countRow = data.__len__()
        # countCol = data.shape[1]
        # headers = data.columns.tolist()
        # self.tableWidget.setRowCount(countRow)
        # self.tableWidget.setColumnCount(countCol)
        # for r in range(countRow):
        #     for c in range(countCol):
        #         self.tableWidget.setItem(r, c, QTableWidgetItem(str(data.ix[r, c])))
        #
        # self.tableWidget.setHorizontalHeaderLabels(headers)
        # self.tableWidget.resizeColumnsToContents()
        # self.tableWidget.resizeRowsToContents()
        # self.tableWidget.move(0, 0)
        #
        # self.layout.addWidget(self.tableWidget)
        # self.setLayout(self.layout)
        # self.show()
        self.second_win = ShowLib_subwindow()
        self.second_win.show()

    def transfer2lines(self):
        P = Process_Method()
        P.transfer2lines(self.file_name5_1)
        QMessageBox.about(self, 'Message', 'File generated successfully!')

    def transfer2stream(self):
        P = Process_Method()
        P.transfer2stream(self.file_name5_1)
        QMessageBox.about(self, 'Message', 'File generated successfully!')


class MyTabWidget(QTabWidget):
    def __init__(self, parent):
        super(QWidget, self).__init__(parent)
        self.layout = QGridLayout(self)
        # self.layout.setSpacing(2)

        self.tabs = QTabWidget()
        self.tab1 = QWidget()
        self.tab2 = QWidget()
        self.tab3 = QWidget()
        self.tab4 = QWidget()

        self.tabs.addTab(self.tab1, 'Process MAS')
        self.tabs.addTab(self.tab2, 'MAS Sign Off')
        self.tabs.addTab(self.tab3, 'Sign-off and PA Roster')
        self.tabs.addTab(self.tab4, 'Check Payment')

        self.bool_to837_file = False
        self.filenameTab3_3 = None
        self.ifcollect270 = False
        self.only270 = False

        self.mytab1()
        self.mytab2()
        self.mytab3()
        self.mytab4()

        ############### add tabs to Widget
        self.layout.addWidget(self.tabs)
        self.setLayout(self.layout)

################ tab 1  Process MAS from RAW DATA
    def mytab1(self):
        self.tab1.layout = QGridLayout(self)
        nameLabel = QLabel('MAS:')
        self.textbox = QLineEdit()
        btnSelect = QPushButton('...')
        btnSelect.clicked.connect(self.select_file)
        btnQuit = QPushButton('Quit')
        btnQuit.clicked.connect(self.close_application)
        btnRun1 = QPushButton('Run')
        btnRun1.clicked.connect(self.mas_raw_process)

        self.checkboxTab1 = QCheckBox('Collect Data For 270?', self)
        self.checkboxTab1.stateChanged.connect(self.SwitchToCollect270)

        self.checkboxTab1_2 = QCheckBox('Only Collect Data For 270', self)
        self.checkboxTab1_2.stateChanged.connect(self.OnlyCollect270)

        self.tab1.layout.addWidget(nameLabel, 0, 0)
        self.tab1.layout.addWidget(self.textbox, 0, 1)
        self.tab1.layout.addWidget(btnSelect, 0, 2)
        self.tab1.layout.addWidget(self.checkboxTab1, 1, 1)
        self.tab1.layout.addWidget(self.checkboxTab1_2, 2, 1)
        self.tab1.layout.addWidget(btnQuit, 3, 0)
        self.tab1.layout.addWidget(btnRun1, 0, 3)
        self.tab1.setLayout(self.tab1.layout)

################ tab 2 Sign off
    def mytab2(self):
        self.tab2.layout = QGridLayout(self)
        nameLabel1 = QLabel('Processed MAS:')
        self.textbox1 = QLineEdit()
        btnSelect1 = QPushButton('...')
        btnSelect1.clicked.connect(self.select_file1)
        nameLabel2 = QLabel('Total Jobs:')
        self.textbox2 = QLineEdit()
        btnSelect2 = QPushButton('...')
        btnSelect2.clicked.connect(self.select_file2)
        btnRun2 = QPushButton('Run')
        btnRun2.clicked.connect(self.mas_sign_off)
        btnQuit2 = QPushButton('Quit')
        btnQuit2.clicked.connect(self.close_application)

        # self.checkboxTab2 = QCheckBox('Also generate 837 file?', self)
        # self.checkboxTab2.stateChanged.connect(self.clickbox)

        self.tab2.layout.addWidget(nameLabel1, 0, 0)
        self.tab2.layout.addWidget(self.textbox1, 0, 1)
        self.tab2.layout.addWidget(btnSelect1, 0, 2)
        self.tab2.layout.addWidget(nameLabel2, 1, 0)
        self.tab2.layout.addWidget(self.textbox2, 1, 1)
        self.tab2.layout.addWidget(btnSelect2, 1, 2)
        self.tab2.layout.addWidget(btnRun2, 2, 1)

        self.tab2.layout.addWidget(btnQuit2, 3, 0)
        self.tab2.setLayout(self.tab2.layout)

################### tab3 SIGN OFF compares with PA roast ################
    def mytab3(self):

        self.tab3.layout = QGridLayout(self)
        nameLabelTab3_1 = QLabel('Sign-off:')
        self.textboxTab3_1 = QLineEdit()
        btnSelectTab3_1 = QPushButton('...')
        btnSelectTab3_1.clicked.connect(self.select_fileTab3_1)
        nameLabelTab3_2 = QLabel('PA Roster:')
        self.textboxTab3_2 = QLineEdit()
        btnSelectTab3_2 = QPushButton('...')
        btnSelectTab3_2.clicked.connect(self.select_fileTab3_2)
        btnRunTab3 = QPushButton('Run')
        btnRunTab3.clicked.connect(self.signoff_W_PA)
        btnQuitTab3 = QPushButton('Quit')
        btnQuitTab3.clicked.connect(self.close_application)

        self.checkboxTab3 = QCheckBox('Collect data for 837?', self)
        self.checkboxTab3.stateChanged.connect(self.clickbox)

        self.nameLabelTab3_3 = QLabel('Processed MAS:')
        self.textboxTab3_3 = QLineEdit()
        self.btnSelectTab3_3 = QPushButton('...')
        self.btnSelectTab3_3.clicked.connect(self.select_fileTab3_3)

        self.nameLabelTab3_3.hide()
        self.textboxTab3_3.hide()
        self.btnSelectTab3_3.hide()

        self.tab3.layout.addWidget(nameLabelTab3_1, 0, 0)
        self.tab3.layout.addWidget(self.textboxTab3_1, 0, 1)
        self.tab3.layout.addWidget(btnSelectTab3_1, 0, 2)
        self.tab3.layout.addWidget(nameLabelTab3_2, 1, 0)
        self.tab3.layout.addWidget(self.textboxTab3_2, 1, 1)
        self.tab3.layout.addWidget(btnSelectTab3_2, 1, 2)

        self.tab3.layout.addWidget(self.nameLabelTab3_3, 2, 0)
        self.tab3.layout.addWidget(self.textboxTab3_3, 2, 1)
        self.tab3.layout.addWidget(self.btnSelectTab3_3, 2, 2)

        self.tab3.layout.addWidget(btnRunTab3, 3, 1)
        self.tab3.layout.addWidget(self.checkboxTab3, 3, 0)
        self.tab3.layout.addWidget(btnQuitTab3, 4, 0)
        self.tab3.setLayout(self.tab3.layout)

    def mytab4(self):
        self.tab4.layout = QGridLayout(self)
        nameLabelTab4_1 = QLabel('MAS Correction:')
        self.textboxTab4_1 = QLineEdit()
        btnSelectTab4_1 = QPushButton('...')
        btnSelectTab4_1.clicked.connect(self.select_fileTab4_1)
        nameLabelTab4_2 = QLabel('Payment:')
        self.textboxTab4_2 = QLineEdit()
        btnSelectTab4_2 = QPushButton('...')
        btnSelectTab4_2.clicked.connect(self.select_fileTab4_2)
        btnRunTab4 = QPushButton('Run')
        btnRunTab4.clicked.connect(self.compare_after_payment)
        btnQuitTab4 = QPushButton('Quit')
        btnQuitTab4.clicked.connect(self.close_application)

        self.tab4.layout.addWidget(nameLabelTab4_1, 0, 0)
        self.tab4.layout.addWidget(self.textboxTab4_1, 0, 1)
        self.tab4.layout.addWidget(btnSelectTab4_1, 0, 2)
        self.tab4.layout.addWidget(nameLabelTab4_2, 1, 0)
        self.tab4.layout.addWidget(self.textboxTab4_2, 1, 1)
        self.tab4.layout.addWidget(btnSelectTab4_2, 1, 2)
        self.tab4.layout.addWidget(btnRunTab4, 3, 1)
        self.tab4.layout.addWidget(btnQuitTab4, 4, 0)
        self.tab4.setLayout(self.tab4.layout)

    def select_file(self):
        self.file_name, _ = QFileDialog.getOpenFileName(self, 'Select File', options=QFileDialog.DontUseNativeDialog)
        self.textbox.setText(self.file_name)

    def select_file1(self):
        self.file_name1, _ = QFileDialog.getOpenFileName(self, 'Select File', options=QFileDialog.DontUseNativeDialog)
        self.textbox1.setText(self.file_name1)

    def select_file2(self):
        self.file_name2, _ = QFileDialog.getOpenFileName(self, 'Select File', options=QFileDialog.DontUseNativeDialog)
        self.textbox2.setText(self.file_name2)

    def close_application(self):
        choice = QMessageBox.question(self, 'Message',
                                     "Are you sure to quit?", QMessageBox.Yes |
                                     QMessageBox.No, QMessageBox.No)

        if choice == QMessageBox.Yes:
            sys.exit()
        else:
            pass

    def SwitchToCollect270(self, state):
        if state == Qt.Checked:
            self.ifcollect270 = True
        else:
            self.ifcollect270 = False

    def OnlyCollect270(self, state):
        if state == Qt.Checked:
            self.only270 = True
        else:
            self.only270 = False


    def clickbox(self, state):
        if state == Qt.Checked:
            self.nameLabelTab3_3.show()
            self.textboxTab3_3.show()
            self.btnSelectTab3_3.show()
            self.bool_to837_file = True

        else:
            self.nameLabelTab3_3.hide()
            self.textboxTab3_3.hide()
            self.btnSelectTab3_3.hide()
            self.bool_to837_file = False

    def mas_raw_process(self):
        choice = QMessageBox.question(self, 'Message', 'Are you sure to process raw data?',
                                      QMessageBox.Yes | QMessageBox.No, QMessageBox.No)

        if choice == QMessageBox.Yes:
            p = Process_MAS(self.file_name)

            if self.only270 == True:
                p.generate_270_data(tofile=True)

            elif self.ifcollect270 == True:
                p.generate_270_data(tofile=True)
                p.add_codes(tofile=True)

            sleep(0.5)
            QMessageBox.about(self, 'Message', 'File generated successfully!')

        else:
            pass

    def mas_sign_off(self):
        choice = QMessageBox.question(self, 'Message', 'Are you sure to process raw data?',
                                      QMessageBox.Yes | QMessageBox.No, QMessageBox.No)

        if choice == QMessageBox.Yes:
            S = SignoffAndCompare()
            S.sign_off(self.file_name1, self.file_name2, tofile=True)
            sleep(0.5)
            QMessageBox.about(self, 'Message', 'File generated successfully!')

        else:
            pass

    def select_fileTab3_1(self):
        self.filenameTab3_1, _ = QFileDialog.getOpenFileName(self, 'Select File', options=QFileDialog.DontUseNativeDialog)
        self.textboxTab3_1.setText(self.filenameTab3_1)

    def select_fileTab3_2(self):
        self.filenameTab3_2, _ = QFileDialog.getOpenFileName(self, 'Select File', options=QFileDialog.DontUseNativeDialog)
        self.textboxTab3_2.setText(self.filenameTab3_2)

    def select_fileTab3_3(self):
        self.filenameTab3_3, _ = QFileDialog.getOpenFileName(self, 'Select File', options=QFileDialog.DontUseNativeDialog)
        self.textboxTab3_3.setText(self.filenameTab3_3)

    def select_fileTab4_1(self):
        self.filenameTab4_1, _ = QFileDialog.getOpenFileName(self, 'Select File', options=QFileDialog.DontUseNativeDialog)
        self.textboxTab4_1.setText(self.filenameTab4_1)

    def select_fileTab4_2(self):
        self.filenameTab4_2, _ = QFileDialog.getOpenFileName(self, 'Select File', options=QFileDialog.DontUseNativeDialog)
        self.textboxTab4_2.setText(self.filenameTab4_2)

    def compare_after_payment(self):
        choice = QMessageBox.question(self, 'Message', 'Are you sure to use payment file to compare?',
                                      QMessageBox.Yes | QMessageBox.No, QMessageBox.No)

        if choice == QMessageBox.Yes:
            S = SignoffAndCompare()
            S.new_compare_after_payment(signoff_compare_PA_file=self.filenameTab4_1, payment_raw_file=self.filenameTab4_2)
            sleep(0.5)
            QMessageBox.about(self, 'Message', 'File generated successfully!')

        else:
            pass

    def signoff_W_PA(self):
        choice = QMessageBox.question(self, 'Message', 'Are you sure to compare two files?',
                                      QMessageBox.Yes | QMessageBox.No, QMessageBox.No)

        if choice == QMessageBox.Yes:
            S = SignoffAndCompare()
            S.compare_signoff_PA(self.filenameTab3_1, self.filenameTab3_2, tofile=True, to837=self.bool_to837_file, mas_2=self.filenameTab3_3)
            sleep(0.5)
            QMessageBox.about(self, 'Message', 'File Generated Successfully!')

        else:
            pass


class mysqlite():

    def __init__(self, BaseName):
        self.conn = sqlite3.connect(BaseName)
        self.cursor = self.conn.cursor()

    def create_table_271(self, table):
        self.cursor.execute('CREATE TABLE IF NOT EXISTS {0}(InvoiceNumber TEXT, EligibilityResult TEXT, ServiceDate DATE, PatientLN TEXT, PatientFN TEXT, PlanCode TEXT,\
                              Eligible TEXT, CIN TEXT, CoveredCodes TEXT,  DOB DATE, Gender TEXT, \
                               PayerName TEXT, PayerAddress TEXT, PayerTel TEXT, OtherPayer1Name TEXT, OtherPayer1Address TEXT, \
                               OtherPayer1Tel TEXT, OtherPayer1GroupNum TEXT, OtherPayer2Name TEXT, OtherPayer2Address TEXT, \
                               OtherPayer2Tel TEXT, OtherPayer2GroupNum TEXT, OtherPayerPolicyNum TEXT, UpdateDate DATE, PRIMARY KEY(InvoiceNumber))'.format(table))

    def upsert271(self, table, data):
        '''
        :param table: sql table name
        :param data: dataframe like
        :return: None
        '''
        self.create_table_271(table)
        data_len = data.__len__()

        for l in range(data_len):
            row_data = data.ix[l, :]

            invoice_num = row_data['Invoice number']
            eligibility_result = row_data['Eligibility Result']
            CIN = row_data['CIN']
            service_date = row_data['Service date']
            plan_code = row_data['Plan code']
            eligible = row_data['Eligible']
            covered_codes = row_data['Covered Codes']
            patient_ln = row_data['Patient firstname']
            patient_fn = row_data['Patient lastname']
            dob = row_data['Patient DOB']
            gender = row_data['Patient gender']
            payer_name = row_data['Payer name']
            payer_address = row_data['Payer address']
            payer_tel = row_data['Contact Tel.']
            otherpayer1name = row_data['Other Payer1 name']
            otherpayer1address = row_data['Other Payer1 address']
            otherpayer1tel = row_data['Other Payer1 tel.']
            otherpayer1groupnum = row_data['Other Payer1 group number']
            otherpayer2name = row_data['Other Payer2 name']
            otherpayer2address = row_data['Other Payer2 address']
            otherpayer2tel = row_data['Other Payer2 tel.']
            otherpayer2groupnum = row_data['Other Payer2 group number']
            otherpayerpolicynum = row_data['Other Payer policy number']
            update_date = datetime.today().date()


            self.cursor.execute('INSERT OR REPLACE INTO {0} (InvoiceNumber, EligibilityResult, ServiceDate, PatientLN, PatientFN, PlanCode, Eligible, CIN, CoveredCodes, DOB, \
                                    Gender, PayerName, PayerAddress, PayerTel, OtherPayer1Name, OtherPayer1Address, OtherPayer1Tel, OtherPayer1GroupNum, \
                                    OtherPayer2Name, OtherPayer2Address, OtherPayer2Tel, OtherPayer2GroupNum, OtherPayerPolicyNum, UpdateDate) \
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'.format(table),
                (invoice_num, eligibility_result, service_date, patient_ln, patient_fn, plan_code, eligible, CIN, covered_codes, dob, gender, payer_name, payer_address, payer_tel,
                 otherpayer1name, otherpayer1address, otherpayer1tel, otherpayer1groupnum, otherpayer2name, otherpayer2address, otherpayer2tel, otherpayer2groupnum, otherpayerpolicynum, update_date))

        self.conn.commit()

    def create_table_manuallyCheck_lib(self, table):
        self.cursor.execute('CREATE TABLE IF NOT EXISTS {0}(Eligible TEXT, PatientLN TEXT, PatientFN TEXT, CIN TEXT, Description TEXT, UpdateDate DATE, PRIMARY KEY(CIN))'.format(table))

    def manuallyUpsert271Lib(self, table, eligible, patient_ln, patient_fn, cin, description):
        self.create_table_manuallyCheck_lib(table)
        update_date = datetime.today().date()
        self.cursor.execute('INSERT OR REPLACE INTO {0} (Eligible, PatientLN, PatientFN, CIN, Description, UpdateDate) VALUES (?,?,?,?,?,?)'.format(table),
                            (eligible, patient_ln, patient_fn, cin, description, update_date))
        self.conn.commit()

    def delete_manually271Lib(self, table, cin):
        self.cursor.execute('DELETE FROM {0} WHERE cin="{1}"'.format(table, cin))
        self.conn.commit()

    def delete_all_manually271Lib(self, table):
        self.cursor.execute('DELETE FROM {0}'.format(table))
        self.conn.commit()

    def generate_excel_from_manually271Lib(self, table, tofile=True):
        date = datetime.today().date()
        df = pd.read_sql('SELECT * FROM {0}'.format(table), con=self.conn)
        if tofile==True:
            df.to_excel('Manually Checking Lib-' + str(date) + '.xlsx', index=False)
        return df

    def create_table_271_plancodes(self, table):
        self.cursor.execute('CREATE TABLE IF NOT EXISTS {0}(PlanCode TEXT, ProviderName TEXT, Telephone TEXT, PlanType TEXT, PRIMARY KEY(PlanCode))'.format(table))

    def upsert_271_plancodes(self, table, plancode, providername, tel, plantype):
        '''
        :param table: table name in sqlite database
        :param data: string
        :return: insert or replace data in table
        '''
        # data_len = data.__len__()
        #
        # for l in range(data_len):
        #     row_data = data.ix[l, :]
        #     plancode = row_data['PlanCode']
        #     providername = row_data['ProviderName']
        #     tel = row_data['Telephone']
        #     plantype = row_data['PlanType']
        self.create_table_271_plancodes(table)
        self.cursor.execute('INSERT OR REPLACE INTO {0} (PlanCode, ProviderName, Telephone, PlanType) VALUES (?, ?, ?, ?)'.format(table),
              (plancode, providername, tel, plantype))

        self.conn.commit()

    def delete_271_plancodes(self, table, plancode):
        '''
        :param table: table name in sqlite database
        :param plancode: plan code, string type
        :return: delete plan code in table
        '''
        self.cursor.execute('DELETE FROM {0} WHERE PlanCode={1}'.format(table, plancode))
        self.conn.commit()

    def get_data_from_271_plancode(self, table):
        df = pd.read_sql('SELECT * FROM {0}'.format(table), con=self.conn)
        return df

    def IfplancodeInDB(self, table, plancode):
        self.cursor.execute('SELECT PlanCode FROM {0} WHERE PlanCode="{1}"'.format(table, plancode))
        res = self.cursor.fetchone()
        if not res:
            return False
        else:
            return True

    def create_table_276277(self, table):
        self.cursor.execute('CREATE TABLE IF NOT EXISTS {0}(Code TEXT, Description TEXT, PRIMARY KEY(Code))'.format(table))

    def upsert_276277_codes(self, table, code, description):
        self.create_table_276277(table)
        self.cursor.execute('INSERT OR REPLACE INTO {0} (Code, Description) VALUES (?, ?)'.format(table),
                            (code, description))

        self.conn.commit()

    def create_table_for_addbase(self, table):
        self.cursor.execute('CREATE TABLE IF NOT EXISTS {0}(BaseName TEXT, BaseAddress TEXT, City TEXT, State TEXT, zipcode TEXT, ETIN TEXT, NPI TEXT, MedicaidProviderNum TEXT, TaxID TEXT, ContactName TEXT, ContactTel TEXT, LocationCode TEXT, PRIMARY KEY(NPI))'.format(table))

    def upsert_newbase(self, table, basename, baseaddress, city, state, zipcode, etin, npi, medicaid_provider_num, taxid, contactname, contactTel, locationcode):
        self.create_table_for_addbase(table)
        self.cursor.execute('INSERT OR REPLACE INTO {0} (BaseName, BaseAddress, City, State, zipcode, ETIN, NPI, MedicaidProviderNum, TaxID, ContactName, ContactTel, LocationCode) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)'.format(table),
                            (basename, baseaddress.upper(), city.upper(), state, zipcode, etin, npi, medicaid_provider_num, taxid, contactname.upper(), contactTel, locationcode))
        self.conn.commit()

    def delete_newbase(self, table, npi):
        self.cursor.execute('DELETE FROM {0} WHERE NPI="{1}"'.format(table, npi))
        self.conn.commit()

    def create_table_for_driver(self, table):
        self.cursor.execute('CREATE TABLE IF NOT EXISTS {0}(Fleet TEXT, Base TEXT, FirstName TEXT, LastName TEXT, DRIVER_ID INTEGER, VEHICLE_ID TEXT, PRIMARY KEY(Fleet))'.format(table))

    def upsert_newdriver(self, table, fleet, base, firstname, lastname, driverid, vehicleid):
        self.create_table_for_driver(table)
        self.cursor.execute('INSERT OR REPLACE INTO {0} (Fleet, Base, FirstName, LastName, DRIVER_ID, VEHICLE_ID) VALUES (?,?,?,?,?,?)'.format(table),
                            (fleet.upper(), base, firstname.upper(), lastname.upper(), driverid, vehicleid.upper()))
        self.conn.commit()

    def delete_driver(self, table, fleet):
        self.cursor.execute('DELETE FROM {0} WHERE Fleet="{1}"'.format(table, fleet))
        self.conn.commit()

    def get_data_from_driver(self, table):
        df = pd.read_sql('SELECT * FROM {0}'.format(table), con=self.conn)
        return df



if __name__ == '__main__':

    current_path = os.getcwd()
    daily_folder = str(datetime.today().date())
    file_saving_path = os.path.join(current_path, daily_folder)
    if not os.path.exists(file_saving_path):
        os.makedirs(file_saving_path)
        print('Save files to {0}'.format(file_saving_path))


    def run():
        SQ = mysqlite('EDI.db')
        app = QApplication(sys.argv)
        Gui = window()
        SQ.cursor.close()
        SQ.conn.close()
        sys.exit(app.exec_())

    run()

