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

from pathlib import Path

import pandas as pd
from scipy.io import loadmat
from tqdm import tqdm

from .helper_code import *

from parameters import DefaultArguments

datasets_path = Path('./datasets')
csv_summaries = Path('/csv_summaries')

same_diag_dict = {'164909002': '733534002',
                  '59118001': '713427006',
                  '63593006': '284470004',
                  '17338001': '427172004'}


def get_recording(file, selected_leads):
    """ Get ecg time series from mat files"""
    num_leads = len(selected_leads)
    mat_recording = loadmat(file)['val']
    num_samples = np.shape(mat_recording)[1]
    chosen_recording = np.zeros((num_leads, num_samples), mat_recording.dtype)
    available_leads = DefaultArguments.twelve_leads  # all recording files have 12 leads
    for i, lead in enumerate(selected_leads):
        if lead in available_leads:
            j = available_leads.index(lead)
            chosen_recording[i, :] = mat_recording[j, :]
    return chosen_recording


def prepare_summary_csv():
    """ Prepare csv summary with all data for each entry except for the ecg time series"""
    csv_summaries.mkdir(exist_ok=True)
    print('Preparation of headers summaries per dataset...')
    unscored_df = pd.read_csv('./data_access/unscored.csv')

    # Ningbo dataset has a label code (251238007, 251211000, 6180003) not reported neither on scored nor on uscored
    # official csv.
    unscored_df = unscored_df.append({'SNOMEDCTCode': 251238007}, ignore_index=True)
    unscored_df = unscored_df.append({'SNOMEDCTCode': 251211000}, ignore_index=True)
    unscored_df = unscored_df.append({'SNOMEDCTCode': 6180003}, ignore_index=True)
    unscored_df['SNOMEDCTCode'] = unscored_df['SNOMEDCTCode'].astype(int)

    for ds in DefaultArguments.all_ds:
        ds_path = str(datasets_path / ds)
        unscored = 0
        for subdir, dirs, files in os.walk(ds_path):
            files = list(filter(lambda x: x.endswith('.hea'), files))
            id, age, sex, dx, rx, sx, hx, freq, num_samples, leads, duration, baselines, adcs = ([] for _ in range(13))
            for file in tqdm(files):
                header = load_header(os.path.join(subdir, file))
                labels = preprocess_labels(get_covariates(header, '#Dx'), unscored_df)
                if not labels:
                    unscored += 1
                    continue
                id.append(get_recording_id(header))
                age.append(get_age(header))
                sex.append(get_encoded_sex(header))
                dx.append(labels)
                baselines.append(list(map(str, get_baselines(header, DefaultArguments.twelve_leads))))
                adcs.append(list(map(str, get_adc_gains(header, DefaultArguments.twelve_leads))))
                freq.append(get_frequency(header))
                num_samples.append(get_num_samples(header))
                leads.append(len(get_leads(header)))
                duration.append(get_num_samples(header) / get_frequency(header))

        d = {'id': id, 'age': age, 'sex': sex, 'dx': dx, 'freq': freq,
             'num_samples': num_samples, 'leads': leads, 'duration': duration,
             'baselines': baselines, 'adcs': adcs}

        # unscored stats
        with open(csv_summaries / str(f'unscored_summary.txt'), 'a') as f:
            f.write(f'{ds} - Out of {str(len(files))} entries {unscored} had only unscored labels and were removed. \n')
        f.close()
        df = pd.DataFrame.from_dict(d)
        df.to_csv(csv_summaries / str(f'summary_{ds}.csv'))


def get_encoded_sex(header):
    """  Extract sex. Encode as 0 for female, 1 for male, and NaN for other. """
    sex = get_sex(header)
    if sex in ('Female', 'female', 'F', 'f'):
        sex = 0
    elif sex in ('Male', 'male', 'M', 'm'):
        sex = 1
    else:
        sex = float('nan')

    return sex


def get_covariates(header, code='#Dx'):
    """ Get covariates from header files """
    covariate = list()
    for l in header.split('\n'):
        if l.startswith(code):
            try:
                entries = l.split(': ')[1].split(',')
                for entry in entries:
                    covariate.append(entry.strip())
            except:
                pass
    return covariate


def preprocess_labels(labels, unscored_df):
    labels = clean_label(labels)  # remove undesired '' cases
    labels = remove_unscored(labels, unscored_df)  # remove unscored labels
    labels = switch_same_diagnosis(labels)  # switch to same diagnosis code
    return labels


def clean_label(labels):
    """ Remove undesired '' cases """
    labels = list(filter(''.__ne__, labels))
    return labels


def remove_unscored(labels, unscored):
    """ Remove unscored labels. See https://github.com/physionetchallenges/evaluation-2021 """
    diagnoses = list(map(int, labels))
    cleaned = []
    for d in diagnoses:
        if d not in unscored['SNOMEDCTCode'].unique():
            cleaned.append(d)
    cleaned = list(map(str, cleaned))
    return cleaned


def switch_same_diagnosis(labels):
    """ Consistent selection of the same diagnosis code for same diagnosis cases. See
    https://github.com/physionetchallenges/evaluation-2021/blob/main/dx_mapping_scored.csv """
    for k, l in enumerate(labels):
        if l in same_diag_dict.keys():
            labels[k] = same_diag_dict[l]
    return labels
