import logging
import os
from collections import Counter
from pprint import pprint
import pandas as pd
import numpy as np
from extractor import parse_file_to_leet

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s'
                              ' - %(levelname)s - %(message)s')

ch.setFormatter(formatter)
logger.addHandler(ch)


def map_root_to_other_logs(whatsapp_logs_dict, base_file):
    files_to_map = filter(lambda x: x != base_file, whatsapp_logs_dict)
    files_to_map = list(files_to_map)
    logger.info('Mapping files: {}'.format(",".join(files_to_map)))
    mapping_dict = create_mapping_dict(base_file, files_to_map,
                                       whatsapp_logs_dict)
    return mapping_dict


def create_mapping_dict(base_file, files_to_map, whatsapp_logs_dict):
    base_df = whatsapp_logs_dict[base_file]
    mapping_dict = dict()
    for fname in files_to_map:
        mapping = map_message_owner_names(base_df, fname, whatsapp_logs_dict)
        mapping_dict[fname] = mapping
    return mapping_dict


def map_message_owner_names(base_df, fname, whatsapp_logs_dict):
    """
    Method to map message_owners from one file to another.
    This is a bit involved so let me explain my reasoning here:
    Everybody can have different names listed for the same contact.
    Problem is, there is no straight way to map one name to another,
    since logs might be broken, unaligend in time or something else.

    The only suitable key to map one name to another would be
    the message content. Problem is a lot of the message content is
    also the same (e.g. everybody writes 420)
    The idea here is that people are inherently unique in what they
    are writing and when they are writing it. So even though one message
    is not enough to identify a person unique all of them are since,
    (thankfully) people are not only writing the same message every time.

    :param base_df: (pd.DataFrame) base dataframe used for mapping
    :param fname: filename of base file
    :param whatsapp_logs_dict: complete data dict containing all logs
    :return: (dict) mapping from one contact to another
    """
    logger.info('Finding Conversion for: {}'.format(fname))
    map_df = whatsapp_logs_dict[fname]

    merged_dfs = pd.merge(base_df, map_df,
                          on=['message_text', 'day', 'month', 'year'])
    cnts = (merged_dfs
            .groupby('message_owner_x')
            .message_owner_y
            .value_counts(normalize=True))

    mapping = list()
    for key, group in cnts.groupby(level=0):
        mapping.append(group.idxmax()[::-1])
    mapping = dict(mapping)
    logger.info('Created conversion: {}'.format(mapping))
    return mapping

def map_names(whatsapp_logs_dict):
    base_file = find_root_mapping_file(whatsapp_logs_dict)
    logger.info('Using {} as mapping root'.format(base_file))
    mapping = map_root_to_other_logs(whatsapp_logs_dict, base_file)
    for key in mapping:
        conversion = mapping[key]
        df = whatsapp_logs_dict[key]
        df['message_owner'] = df['message_owner'].apply(conversion.get)
        whatsapp_logs_dict[key] = df
        logger.info('Converted names in file: {}'.format(key))
    return mapping


def find_root_mapping_file(whatsapp_logs_dict):
    max_uniques = 0
    utv_ratio = 0
    base_file = None
    for key in whatsapp_logs_dict:
        unique_owners = whatsapp_logs_dict[key]['message_owner'].unique()
        valid_names = [x for x in unique_owners
                       if not any(c.isdigit() for c in x)]
        valid_users = len(valid_names)
        unique_to_valid_ratio = valid_users / float(len(unique_owners))

        logger.info('Total-Users: {},Valid-Users: {}, '
                    'UTV-Ratio: {}, file: {}'.format(len(unique_owners),
                                                     valid_users,
                                                     unique_to_valid_ratio,
                                                     key))

        if valid_users >= max_uniques and unique_to_valid_ratio >= utv_ratio:
            max_uniques = valid_users
            base_file = key
            utv_ratio = unique_to_valid_ratio
    return base_file


def get_section_winners(day, month, year, parsed_files_dict, feat_key):
    scaffold_df = pd.DataFrame()
    for key in parsed_files_dict:
        file_df = parsed_files_dict[key]
        mask = get_date_mask(day, file_df, month, year)
        sel_df = file_df[mask]
        occ_count = (sel_df
                     .groupby('message_owner')[feat_key]
                     .sum())

        # Filter possible duplicates
        occ_count[occ_count > 1] = 1
        scaffold_df[key] = occ_count
    aggregated = scaffold_df.mean(axis=1)
    # in dubio pro reo
    winners = aggregated[aggregated >= 0.5].index.tolist()
    return winners


def get_winner_420(day,month,year,parsed_files_dict):
    scaffold_df = pd.DataFrame()
    for key in parsed_files_dict:
        file_df = parsed_files_dict[key]
        mask = get_date_mask(day, file_df, month, year)
        mask_16 = mask & (file_df.hour == 16)
        mask_04 = mask & (file_df.hour == 4)

        occs_420 = calc_occurences_420(file_df, mask_04,mask_16)
        scaffold_df[key] = occs_420

    retval =list()
    form_vals = np.round(scaffold_df.mean(axis=1).fillna(0),0).astype(int)
    for key,val in form_vals.items():
        retval.extend([key] * val)

    return retval


def calc_occurences_420(file_df, mask_04, mask_16):
    occ_count_04 = (file_df[mask_04]
                    .groupby('message_owner')
                    .is_420
                    .sum())
    occ_count_04[occ_count_04 > 1] = 1

    occ_count_16 = (file_df[mask_16]
                    .groupby('message_owner')
                    .is_420
                    .sum())
    occ_count_16[occ_count_16 > 1] = 1

    scaff = pd.DataFrame()
    scaff['04'] = occ_count_04
    scaff['16'] = occ_count_16
    occs_420 = scaff.sum(axis=1)
    return occs_420





def get_date_mask(day, file_df, month, year):
    mask = ((file_df.day == day)
            & (file_df.month == month)
            & (file_df.year == year))
    return mask


def count_leet_and_greet(data_folder, start='01-01-2018', end='12-31-2018'):
    parsed_files_dict = parse_log_files(data_folder)
    logger.info('Mapping message owners between files')
    map_names(parsed_files_dict)
    counter_zeet = Counter()
    counter_leet = Counter()
    counter_420 = Counter()

    for date in pd.date_range(start, end):
        logger.info('Calculating winners for: {}'.format(date))
        year = date.year
        month = date.month
        day = date.day
        winner_partial = lambda x: get_section_winners(
            day=day, month=month, year=year,
            parsed_files_dict=parsed_files_dict,
            feat_key=x
        )

        winner_leet = winner_partial('is_leet')
        winner_zeet = winner_partial('is_zeet')
        winner_420 = get_winner_420(day,month,year,parsed_files_dict)
        counter_leet += Counter(winner_leet)
        counter_zeet += Counter(winner_zeet)
        counter_420 += Counter(winner_420)

        logger.info(winner_zeet)
        logger.info(counter_zeet)
    return dict(zeet=counter_zeet, leet=counter_leet, fourtwenty=counter_420)


def parse_log_files(data_folder):
    parsed_files = dict()
    logger.info('Reading from: {}'.format(data_folder))
    data_files = [x for x in os.listdir(data_folder) if x.endswith('.txt')]
    for whatsapp_log in data_files:
        logger.info('Parsing file: {}'.format(whatsapp_log))
        with open(os.path.join(data_folder, whatsapp_log)) as f:
            data = parse_file_to_leet(f.read())
            parsed_files[whatsapp_log] = pd.DataFrame(data)
    return parsed_files


if __name__ == '__main__':
    base_path = os.path.dirname(__file__)
    data_folder = os.path.join(base_path, './data/')
    leet_and_greet_count = count_leet_and_greet(data_folder,start='01-01-2018',
                                                end='12-31-2018')
    pprint(leet_and_greet_count)
