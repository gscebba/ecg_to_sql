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

import sys
from ast import literal_eval
from pathlib import Path

import pandas as pd
import torch
from torch.utils.data import Dataset

from .prepare import get_recording

datasets_path = Path('./datasets')
csv_summaries = Path('/csv_summaries')


class DataBase(Dataset):
    def __init__(self, dataset_name, selected_leads, ds_portion=1.0, random_seed=42):
        try:
            self.df = pd.read_csv(csv_summaries / f'summary_{dataset_name}.csv')
        except:
            print(f'ERR: {dataset_name} csv summary not found. Run prepare.prepare_summary_csv', file=sys.stdout)
            sys.exit(1)

        self.dataset_name = dataset_name
        self.dataset = datasets_path / dataset_name
        self.leads = selected_leads

        # random sample dataset, for testing purposes
        if ds_portion < 1.0:
            self.df = self.df.sample(frac=ds_portion, random_state=random_seed)
        elif ds_portion > 1:
            self.df = self.df.sample(n=ds_portion, random_state=random_seed)
        self.df.reset_index(inplace=True)

    def __len__(self):
        return len(self.df)

    def __getitem__(self, item):

        # patient_id
        patient_id = self.df['id'][item]

        # ecg leads loading
        ecg_leads = get_recording(file=self.dataset / str(self.df['id'][item] + '.mat'),
                                  selected_leads=self.leads)

        # target
        labels = literal_eval(self.df['dx'][item])

        # baseline adc data
        baselines, adcs = literal_eval(self.df['baselines'][item]), literal_eval(self.df['adcs'][item])
        baselines, adcs = list(map(float, baselines)), list(map(float, adcs))

        # resampling
        sampling_frequency = self.df['freq'][item]
        ecg_leads = resample(ecg_leads, sampling_frequency)

        # meta data ,id,age,sex,dx,freq,num_samples,leads,duration,baselines,adcs
        num_samples = ecg_leads.shape[1]
        num_leads = self.df['leads'][item]
        duration = self.df['duration'][item]

        # covariates
        age = self.df['age'][item]
        sex = torch.tensor(self.df['sex'][item])

        # Note: for model training tasks consider to add to this pipeline time series padding,
        # normalization, one-hot encoding.

        return patient_id, ecg_leads, labels, age, sex, baselines, adcs, num_samples, num_leads, duration


def resample(data, sample_frequency):
    """
    Resample at 250 Hz
    :param data: ecg leads
    :param sample_frequency: raw sample frequency
    :return:
    """
    if sample_frequency == 500:
        resample_factor = 2
    elif sample_frequency == 1000:
        resample_factor = 4
    elif sample_frequency == 257:
        return data, sample_frequency
    resampled = data[:, ::resample_factor]
    return resampled
