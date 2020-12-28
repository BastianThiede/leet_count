import csv
import logging
import os
from collections import Counter
from pprint import pprint

import numpy as np
import pandas as pd
from extractor import parse_file_to_leet

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s'
                              ' - %(levelname)s - %(message)s')

ch.setFormatter(formatter)
logger.addHandler(ch)


def map_root_to_other_logs(parsed_whatsapp_logs, base_file):
    """
    Combining function that filters before using the base file to find
    mappings from a message owner in one logfile to a message owner in another

    :param parsed_whatsapp_logs: (dict)
    {'filename': (pd.DataFrame),...}
    :param base_file:  (str) filename of base_file
    :return: (dict)
    {
        (str) 'to_be_mapped_name': (str) 'basefile_message_owner',
        ...
    }
    """
    files_to_map = filter(lambda x: x != base_file, parsed_whatsapp_logs)
    files_to_map = list(files_to_map)
    logger.info('Mapping files: {}'.format(",".join(files_to_map)))
    mapping_dict = create_mapping_dict(base_file, files_to_map,
                                       parsed_whatsapp_logs)
    return mapping_dict


def create_mapping_dict(base_file, files_to_map, parsed_whatsapp_logs):
    """
    Function that aggregates the mapped message owners in a dictionary

    :param base_file: (str) Filename of base file
    :param files_to_map: (list) list of filenames to be mapped
    :param parsed_whatsapp_logs: (dict)
    {'filename': (pd.DataFrame),...}
    :return: (dict)
    {
        (str) 'filename': (dict) name mapping,
        ...
    }

    """
    base_df = parsed_whatsapp_logs[base_file]
    mapping_dict = dict()
    for fname in files_to_map:
        mapping = map_message_owner_names(base_df, fname, parsed_whatsapp_logs)
        mapping_dict[fname] = mapping
    return mapping_dict


def map_message_owner_names(base_df, fname, parsed_whatsapp_logs):
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
    :param parsed_whatsapp_logs: complete data dict containing all logs
    :return: (dict) mapping from one contact to another
    """
    logger.info('Finding Conversion for: {}'.format(fname))
    map_df = parsed_whatsapp_logs[fname]

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


def map_names(parsed_whatsapp_logs):
    """
    Function that maps message_owners in all logfiles to the same
    message owner
    :param parsed_whatsapp_logs: (dict)
    {'filename': (pd.DataFrame),...}    :return:
    """
    base_file = find_root_mapping_file(parsed_whatsapp_logs)
    logger.info('Using {} as mapping root'.format(base_file))
    mapping = map_root_to_other_logs(parsed_whatsapp_logs, base_file)
    for key in mapping:
        conversion = mapping[key]
        df = parsed_whatsapp_logs[key]
        df['message_owner'] = df['message_owner'].apply(conversion.get)
        parsed_whatsapp_logs[key] = df
        logger.info('Converted names in file: {}'.format(key))
    return mapping


def find_root_mapping_file(parsed_whatsapp_logs):
    """
    Function to determine which file to use as a mapping root for all
    names. The logic here is as follows, a file is a good candidate
    if it doesn't contain a lot of names that do no contain any
    numbers (i.e.: phone numbers) and additionally have a ratio between
    all unique users and valid user names (those without numbers) close
    to one. The reason for this is that some people might switch
    numbers which would increase the number of unique users, the utv-ratio
    counteracts this effect.

    :param parsed_whatsapp_logs: (dict)
    {'filename': (pd.DataFrame),...}

    :return: (str) basefile name
    """
    max_uniques = 0
    utv_ratio = 0
    base_file = None
    for key in parsed_whatsapp_logs:
        if parsed_whatsapp_logs[key].empty:
            continue

        unique_owners = parsed_whatsapp_logs[key]['message_owner'].unique()
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


def get_section_winners(day, month, year, parsed_whatsapp_logs, feat_key):
    """
    Calculates the winners of a certain leet/zeet time accross all logs.
    Also solves disputes between mutliple logfile. i.E.: If a zeet is only
    determined as a win if the majority of the logfiles says so.
    If it's a 50:50 split the zeet is counted as a win.
    
    :param day:  (int)
    :param month:  (int)
    :param year:  (int)
    :param parsed_whatsapp_logs:(dict)
    {'filename': (pd.DataFrame),...} 
    :param feat_key: (str) determining which feature should be evaluated
    (feasible for is_zeet and is_leet)
    :return: (list) list of winners
    """
    scaffold_df = pd.DataFrame()
    for key in parsed_whatsapp_logs:
        file_df = parsed_whatsapp_logs[key]
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


def get_winner_420(day, month, year, parsed_whatsapp_logs):
    """
    Special method to determine a 420 message win. Since 420 is possible
    at multiple times it's a little more involved to calculate.
    First determine a winner for 16:20 and 4:20 then aggregate

    :param day: (int)
    :param month: (int)
    :param year: (int)
    :param parsed_whatsapp_logs:(dict)
    {'filename': (pd.DataFrame),...}
    :return: (list) list of winners, if someone has won both 4:20 and 16:20
    the person will appear twice in the list
    """
    scaffold_df = pd.DataFrame()
    for key in parsed_whatsapp_logs:
        file_df = parsed_whatsapp_logs[key]
        mask = get_date_mask(day, file_df, month, year)
        mask_16 = mask & (file_df.hour == 16)
        mask_04 = mask & (file_df.hour == 4)

        occs_420 = calc_occurences_420(file_df, mask_04, mask_16)
        scaffold_df[key] = occs_420

    retval = list()
    form_vals = np.round(scaffold_df.mean(axis=1).fillna(0), 0).astype(int)
    for key, val in form_vals.items():
        retval.extend([key] * val)

    return retval


def calc_occurences_420(file_df, mask_04, mask_16):
    """
    Calculate the occurences for 16:20 and 4:20
    :param file_df: (pd.DataFrame) dataframe of messages
    :param mask_04: (pd.Series of bools) mask to select 4:20 users on a
    certain day
    :param mask_16: (pd.Series of bools) mask to select 16:20 users
    on a certain day
    :return:(pd.Series) Series of Occurences indexed by message owner
    """
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


def count_leet_and_greet(data_folder, start='01-01-2019', end='12-31-2019'):
    """
    Aggregating function to  count leets and greets over the course of a
    given time range. Iterates on timerange calculates winner for each
    day.

    :param data_folder: Folder to parse data from
    :param start: (str) time to start (american time format)
    :param end: (str) time to end (american time format)
    :return: (dict) Dict of aggregated statistics(count) for zeet and greet
    {
        'zeet': (dict) dict with counts per message owner,
        'leet': (dict) dict with counts per message owner,
        'fourtwenty': : (dict) dict with counts per message owner
    }
    """
    parsed_files_dict = parse_log_files(data_folder)
    logger.info('Mapping message owners between files')
    mapping = map_names(parsed_files_dict)
    key = (set(mapping.keys()) ^ set(parsed_files_dict.keys())).pop()
    people = parsed_files_dict[key].message_owner.unique().tolist()
    counter_zeet = Counter()
    counter_leet = Counter()
    counter_420 = Counter()
    data_file_420 = open('/tmp/420.csv', 'w')
    data_file_leet = open('/tmp/leet.csv', 'w')
    data_file_zeet = open('/tmp/zeet.csv', 'w')
    w1 = csv.DictWriter(data_file_420, people)
    w2 = csv.DictWriter(data_file_leet, people)
    w3 = csv.DictWriter(data_file_zeet, people)
    w1.writeheader()
    w2.writeheader()
    w3.writeheader()
    logger.info('Users found: %s', ",".join(mapping))

    for date in pd.date_range(start, end):
        logger.info('Calculating winners for: {}'.format(date))
        year = date.year
        month = date.month
        day = date.day
        winner_partial = lambda x: get_section_winners(
            day=day, month=month, year=year,
            parsed_whatsapp_logs=parsed_files_dict,
            feat_key=x
        )

        winner_leet = winner_partial('is_leet')
        winner_zeet = winner_partial('is_zeet')
        winner_420 = get_winner_420(day, month, year, parsed_files_dict)
        counter_leet += Counter(winner_leet)
        counter_zeet += Counter(winner_zeet)
        counter_420 += Counter(winner_420)
        w1.writerow(counter_420)
        w2.writerow(counter_leet)
        w3.writerow(counter_zeet)

        logger.info(winner_leet)
        logger.info(counter_leet)
    return dict(zeet=counter_zeet, leet=counter_leet, fourtwenty=counter_420)


def parse_log_files(data_folder):
    """
    Function to parse all data from the specified data folder

    :param data_folder: (str) dir-path containing all logs
    :return: (dict) dict of filename to parsed data mapping
    {
        'fname': (pd.DataFrame),
        ...
    }
    """
    parsed_files = dict()
    logger.info('Reading from: {}'.format(data_folder))
    data_files = [x for x in os.listdir(data_folder) if x.endswith('.txt')]
    logger.info(f'Found {len(data_files)} data files!')
    for whatsapp_log in data_files:
        logger.info('Parsing file: {}'.format(whatsapp_log))
        with open(os.path.join(data_folder, whatsapp_log)) as f:
            data = parse_file_to_leet(f.read())
            df = pd.DataFrame(data)
            if df.empty:
                logger.warning('FAILED LOG: {}'.format(whatsapp_log))
                continue
            parsed_files[whatsapp_log] = pd.DataFrame(data)
    return parsed_files


if __name__ == '__main__':
    base_path = os.path.dirname(__file__)
    data_folder = os.path.join(base_path, '../test/test_data')
    leet_and_greet_count = count_leet_and_greet(data_folder,
                                                start='01-01-2019',
                                                end='12-31-2019')
    pprint(leet_and_greet_count)
