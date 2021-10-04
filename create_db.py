#  Copyright (c) 2021. Gaetano Scebba
#  Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
#  documentation files (the "Software"), to deal in the Software without restriction, including without limitation
#  the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software,
#  and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
#
#  The above copyright notice and this permission notice shall be included in all copies or substantial portions
#   of the Software.
#
#  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED
#  TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
#  THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF
#  CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
#  DEALINGS IN THE SOFTWARE.

from db_access import DbAccess
from pathlib import Path
from parameters import DefaultArguments
from data_access.data_access import DataBase
from torch.utils.data import DataLoader
from tqdm import tqdm

import sys
import numpy as np

datasets_path = Path('./data_access/datasets')


class CreateDb(DbAccess):
    TABLE_PATIENTS = 'patients'
    TABLE_DX_DICT = 'diagnosis_dictionary'
    TABLE_DIAGNOSES = 'diagnoses'
    TABLE_DS_DICT = 'dataset_dictionary'

    def __init__(self, data_dir: Path, db_file_name: str):
        super(CreateDb, self).__init__(data_dir, db_file_name)
        self.setup_schema()
        self.dx_list = list(zip(range(len(DefaultArguments.all_labels)), DefaultArguments.all_labels))
        self.ds_list = list(zip(range(len(DefaultArguments.all_ds)), DefaultArguments.all_ds))
        self._populate_dictionaries()

    def setup_schema(self):
        """
        Create all tables except for time series ones.
        :return:
        """
        self.write(f'CREATE TABLE IF NOT EXISTS {self.TABLE_DS_DICT} ('
                   f'ds_id INTEGER PRIMARY KEY AUTOINCREMENT,'
                   f'ds_name CHAR(32))')

        self.write(f'CREATE TABLE IF NOT EXISTS {self.TABLE_DX_DICT} ('
                   f'diagnosis_id INTEGER PRIMARY KEY,'
                   f'dx_code CHAR(32))')

        self.write(f'CREATE TABLE IF NOT EXISTS {self.TABLE_DIAGNOSES} ('
                   f'patient_id INT NOT NULL,'
                   f'diagnosis_id INT,'
                   f'CONSTRAINT diagnosis_id '
                   f'FOREIGN KEY (diagnosis_id)'
                   f'REFERENCES {self.TABLE_DX_DICT} (diagnosis_id));')

        self.write(f'CREATE TABLE IF NOT EXISTS {self.TABLE_PATIENTS} ('
                   f'patient_id INTEGER PRIMARY KEY AUTOINCREMENT,'
                   f'original_id CHAR(32),'
                   f'dataset_id INT,'
                   f'age INT,'
                   f'sex INT,'
                   f'num_leads INT, '
                   f'num_samples INT, '
                   f'duration REAL, '
                   f'bs1 REAL, bs2 REAL, bs3 REAL, bs4 REAL, bs5 REAL, bs6 REAL, '
                   f'bs7 REAL, bs8 REAL, bs9 REAL, bs10 REAL, bs11 REAL, bs12 REAL, '
                   f'ad1 REAL, ad2 REAL, ad3 REAL, ad4 REAL, ad5 REAL, ad6 REAL, '
                   f'ad7 REAL, ad8 REAL, ad9 REAL, ad10 REAL, ad11 REAL, ad12 REAL, '
                   f'CONSTRAINT patient_id '
                   f'FOREIGN KEY (patient_id) '
                   f'REFERENCES {self.TABLE_DIAGNOSES} (patient_id),'
                   f'CONSTRAINT dataset_id '
                   f'FOREIGN KEY (dataset_id) '
                   f'REFERENCES {self.TABLE_DS_DICT} (ds_id));')

    def _setup_data_table(self, original_id: str):
        """
        Create time series table for patient with patient_id.
        :param original_id: Patient id formatted using original file names where time series were stored in.
        :return:
        """

        ecg_leads_table = 'data_' + str(original_id)

        self.write(f'CREATE TABLE IF NOT EXISTS {ecg_leads_table} ('
                   f'time_id INTEGER PRIMARY KEY, '
                   f'lead1 INTEGER, lead2 INTEGER, lead3 INTEGER, lead4 INTEGER, lead5 INTEGER, lead6 INTEGER, '
                   f'lead7 INTEGER, lead8 INTEGER, lead9 INTEGER, lead10 INTEGER, lead11 INTEGER, lead12 INTEGER)')

    def _populate_dictionaries(self):
        """
        Populate dictionary tables with datasets names and diagnoses code.
        :return:
        """

        next_id = self.read(f"SELECT MAX(ds_id) FROM {self.TABLE_DS_DICT}")
        if next_id[0][0] is not None:
            return
        # populate dataset dictionary
        self.write_many(f'INSERT INTO {self.TABLE_DS_DICT} VALUES (?, ?)', self.ds_list)

        # populate diagnosis dictionary
        self.write_many(f'INSERT INTO {self.TABLE_DX_DICT} VALUES (?, ?)', self.dx_list)

    def populate_schema(self, dataset_name: str, ds_portion: int):
        """
        Populate all tables except for time series ones.
        :param dataset_name: Dataset name
        :param ds_portion: Portion of original dataset to sample. If ds_portion = 1 include the whole dataset
        :return:
        """

        dx_dict = dict([(s[1], s[0]) for s in self.dx_list])
        ds_dict = dict([(d[1], d[0]) for d in self.ds_list])

        ds = DataBase(dataset_name=dataset_name, selected_leads=DefaultArguments.twelve_leads,
                      ds_portion=ds_portion)
        dl = DataLoader(ds, shuffle=False)
        ds_id = ds_dict[dataset_name]

        print('Populating patients tables...')
        for pid, _, labels, age, sex, baselines, adcs, num_samples, num_leads, duration in tqdm(dl):
            bs = [b.item() for b in baselines]
            ad = [a.item() for a in adcs]

            age = 'NULL' if np.isnan(age) else age.item()
            sex = 'NULL' if np.isnan(sex) else sex.item()

            # populate patients table
            self.write(f"INSERT INTO {self.TABLE_PATIENTS} "
                       f"(original_id, dataset_id, age, sex, num_leads, num_samples, duration, "
                       f"bs1, bs2, bs3, bs4, bs5, bs6, bs7, bs8, bs9, bs10, bs11, bs12, "
                       f"ad1, ad2, ad3, ad4, ad5, ad6, ad7, ad8, ad9, ad10, ad11, ad12) "
                       f"VALUES "
                       f"('{pid[0]}', {ds_id}, {age}, {sex}, "
                       f"{num_leads.item()}, {num_samples.item()}, {duration.item()}, "
                       f"{bs[0]}, {bs[1]}, {bs[2]}, {bs[3]}, {bs[4]}, {bs[5]}, {bs[6]}, {bs[7]}, {bs[8]}, {bs[9]}, {bs[10]}, {bs[11]}, "
                       f"{ad[0]}, {ad[1]}, {ad[2]}, {ad[3]}, {ad[4]}, {ad[5]}, {ad[6]}, {ad[7]}, {ad[8]}, {ad[9]}, {ad[10]}, {ad[11]})")

            # populate diagnoses table
            labels_id = [dx_dict[l[0]] for l in labels]
            patient_id = self.read(f"SELECT MAX(patient_id) FROM {self.TABLE_PATIENTS}")
            diagnoses_values = [(patient_id[0][0], l) for l in labels_id]

            self.write_many(f"INSERT INTO {self.TABLE_DIAGNOSES} (patient_id, diagnosis_id)"
                            f"VALUES (?, ?)", diagnoses_values)

            # setup data tables
            self._setup_data_table(pid[0])
        print(f'INFO: {dataset_name} schema tables population completed.', file=sys.stdout)

    def populate_data_tables(self, dataset_name: str, ds_portion: int):
        """
        Populate ecg time series tables.
        :param dataset_name: Dataset name
        :param ds_portion: Portion of original dataset to sample. If ds_portion = 1 include the whole dataset
        :return:
        """

        ds = DataBase(dataset_name=dataset_name, selected_leads=DefaultArguments.twelve_leads,
                      ds_portion=ds_portion)
        dl = DataLoader(ds, shuffle=False)

        print('Populating data tables...')
        for pid, ecg, _, _, _, _, _, _, _, _ in tqdm(dl):
            ecg = ecg.squeeze().permute(1, 0).numpy()
            ecg = [tuple(ecg[t, :]) for t in range(ecg.shape[0])]

            self.write_many(f"INSERT INTO data_{pid[0]} (lead1, lead2, lead3, lead4, lead5, lead6, " \
                            f"lead7, lead8, lead9, lead10, lead11, lead12) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", ecg)

        print(f'INFO: {dataset_name} data population completed.', file=sys.stdout)
