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

import pandas as pd

datasets_path = Path('./data_access/datasets')


class LoadDb(DbAccess):
    TABLE_PATIENTS = 'patients'
    TABLE_DX_DICT = 'diagnosis_dictionary'
    TABLE_DIAGNOSES = 'diagnoses'
    TABLE_DS_DICT = 'dataset_dictionary'
    SAMPLING_FREQUENCY = 250

    def __init__(self, data_dir: str, db_file_name: str):
        super(LoadDb, self).__init__(data_dir, db_file_name)

    def get_patients_with_diagnoses(self, to_df: bool = False, n: int = None):
        """
        Get randomly sampled patient ids with corresponding diagnosis code in SNOMEDCTCode format.
        :param to_df: cast output to a DataFrame
        :param n: number of patients
        :return: with to_df = True return dataframe with patient id and diagnosis code
        """
        query = f"SELECT p.patient_id, GROUP_CONCAT(d.dx_code, ',') as 'diagnoses' FROM {self.TABLE_PATIENTS} as p " \
                f"INNER JOIN {self.TABLE_DIAGNOSES} as pd ON p.patient_id = pd.patient_id " \
                f"INNER JOIN {self.TABLE_DX_DICT} as d ON pd.diagnosis_id = d.diagnosis_id " \
                f"GROUP BY p.patient_id " \
                f"ORDER BY RANDOM() " \

        if n is not None:
            query += f"LIMIT {n}"
        data = pd.read_sql_query(query, self.db) if to_df else self.read(query)
        return data

    def get_single_patient_data(self, patient_id: int, to_df: bool = False):
        """
        Get original id and diagnosis code for a single patient.
        :param patient_id: Patient id to retrieve data from
        :param to_df: cast output to a Dataframe
        :return: with to_df = True, return a dataframe with original_id and corresponding diagnosis
        """
        query = f"SELECT p.original_id, GROUP_CONCAT(d.dx_code, ',') as 'diagnosis' FROM {self.TABLE_PATIENTS} as p " \
                f"INNER JOIN {self.TABLE_DIAGNOSES} as pd ON p.patient_id = pd.patient_id " \
                f"INNER JOIN {self.TABLE_DX_DICT} as d ON pd.diagnosis_id = d.diagnosis_id " \
                f"WHERE p.patient_id = {patient_id} " \
                f"GROUP BY p.patient_id " \

        data = pd.read_sql_query(query, self.db) if to_df else self.read(query)
        return data[0]

    def get_covariates(self, patient_ids: tuple, to_df: bool = False):
        """
        Get patients age and sex
        :param patient_ids: list of patient ids to retrieve covariates from
        :param to_df: cast the output to a DataFrame
        :return: with to_df = True, return a dataframe with age and sex of the corresponding patient(s)
        """
        if len(patient_ids) > 1:
            query = f"SELECT age, sex FROM {self.TABLE_PATIENTS} WHERE patient_id IN {patient_ids}"
        else:
            query = f"SELECT age, sex FROM {self.TABLE_PATIENTS} WHERE patient_id = {patient_ids[0]}"
        data = pd.read_sql_query(query, self.db) if to_df else self.read(query)
        return data

        """ Get ecg lead time series. """

    def get_ecg(self, patient_id: int, leads: list = None, window_length: float = None):
        """
        Get ecg lead time series.
        :param patient_id: Patient id to retrieve ecg time series from
        :param leads: List of lead numbers to retrieve. If None retrieve all the leads
        :param window_length: Time series window length in seconds. If None retrieve the whole time series for each lead
        :return: (m, n) numpy array, with m = number of ecg leads and n = time series duration of the retrieved ecg.
        """

        original_id = self.read(f"SELECT original_id FROM {self.TABLE_PATIENTS} WHERE patient_id = {patient_id}")
        leads = tuple([f'lead{l}' for l in range(1, 13)]) if leads is None else tuple([f'lead{l}' for l in leads])
        query = f"SELECT {', '.join([str(i) for i in leads])} FROM data_{original_id[0][0]} "

        if window_length is not None:
            num_samples = int(window_length * self.SAMPLING_FREQUENCY)
            query += f"LIMIT {num_samples}"

        def to_int(b):
            return int.from_bytes(b, byteorder='little', signed=True)

        data = pd.read_sql_query(query, self.db)
        data = data.applymap(to_int).to_numpy()

        return data
