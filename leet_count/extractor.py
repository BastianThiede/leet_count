import os
import re
from datetime import datetime

import pandas as pd


def parse_datetime(some_str):
    """
    Pasing timestamps in logs, since multiple formats need to be handled
    this method tries a couple options to parse.

    :param some_str: (str) whatsapplog line to parse
    :return: datetime or None (if no formatting succeeeded)
    """
    formats = [
        (r'\d{2}\.\d{2}\.\d{2}, \d{2}:\d{2}:\d{2}', '%d.%m.%y, %H:%M:%S'),
        (r'\d{1,2}/\d{1,2}/\d{1,2}, \d{2}:\d{2}', '%m/%d/%y, %H:%M'),
        (r'\d{2}\.\d{2}\.\d{2}, \d{2}:\d{2}', '%d.%m.%y, %H:%M')]
    date = None
    for regex, date_format in formats:
        date_match = re.search(regex, some_str)
        if date_match:
            date = datetime.strptime(date_match.group(), date_format)

    return date


def parse_owner_android(some_str):
    """
    Method that tries to parse the message owner of a log line,
    needs to be also specific to system logs have been generated.

    :param some_str: (str) Whatsapp log line
    :return: (str) owner of message or None
    """
    regex = r' - .+: '
    name_match = re.search(regex, some_str)
    if name_match:
        name = name_match.group()
        # first 2 characters are ] and space, filter those by postition
        name_filter = name[3:-2].split(':')[0]
    else:
        name_filter = None
    return name_filter


def parse_owner(some_str):
    """
    Grouping function to parse either android or ios whatsapp logs
    :param some_str:  (str) Whatsapp log line
    :return: (str) owner of message or None
    """
    name_filter = parse_owner_ios(some_str)
    if name_filter is None:
        name_filter = parse_owner_android(some_str)
    return name_filter


def parse_owner_ios(some_str):
    """
    Method that tries to parse the message owner of a log line,
    needs to be also specific to system logs have been generated.

    :param some_str:  (str) Whatsapp log line
    :return: (str) owner of message or None
    """
    regex = r'] .+: '
    name_match = re.search(regex, some_str)
    if name_match:
        name = name_match.group()
        # first 2 characters are ] and space, filter those by postition
        name_filter = name[2:-2].split(':')[0]
    else:
        name_filter = None
    return name_filter


def parse_text(some_str):
    """
    Tries to parse message content, not used for counting only used
    for matching message owners accross multiple logs

    :param some_str: (str) Whatsapp log line
    :return:
    """
    return some_str.split(': ')[-1].replace('\n', '')


def parse_details(log_line):
    """
    Function that parses date, message owner and message content from
    a whatsapp logline
    :param log_line:  (str) Whatsapp log line
    :return: (dict) parsed message details or None
    """

    try:
        message_time = parse_datetime(log_line)
        message_owner = parse_owner(log_line)
        message_text = parse_text(log_line)

        return dict(message_time=message_time, message_owner=message_owner,
                    message_text=message_text)
    except AttributeError:
        return None


def determine_leet(detail_dict):
    ts = detail_dict['message_time']
    if ts.minute == 37 and ts.hour == 13:
        return 1
    else:
        return 0


def determine_zeet(detail_dict):
    ts = detail_dict['message_time']
    if ts.minute == 37 and ts.hour == 23:
        return 1
    else:
        return 0


def determine_420(detail_dict):
    ts = detail_dict['message_time']
    if ts.minute == 20 and (ts.hour == 16 or ts.hour == 4):
        return 1
    else:
        return 0


def determine_soli(detail_dict):
    message = detail_dict['message_text']
    if 'soli' in message.lower():
        return 1
    else:
        return 0


def determine_fail(detail_dict):
    ts = detail_dict['message_time']
    if ts.minute == 38 and (ts.hour == 23 or ts.hour == 13):
        return 1
    elif ts.minute == 21 and (ts.hour == 16 or ts.hour == 4):
        return 1
    else:
        return 0


def build_leet_features(detail_dict):
    """
    function creating message features (either leet,zeet,420 soli or fail)
    currently soli and fail are not processed by the aggregation script

    :param detail_dict: (dict) message details (time,owner,text)
    {
        'message_time':(datetime),
        'message_text': (str),
        'message_owner' (str)
    }

    :return: (dict) feature dict of notable events
    """
    is_leet = determine_leet(detail_dict)
    is_zeet = determine_zeet(detail_dict)
    is_420 = determine_420(detail_dict)
    is_soli = determine_soli(detail_dict)
    is_fail = determine_fail(detail_dict)
    features = dict(is_leet=is_leet, is_zeet=is_zeet, is_420=is_420,
                    is_soli=is_soli, is_fail=is_fail)
    return features


def parse_file_to_leet(file_content):
    """
    Method that parses file content (str) of a whatsapp log.
    First gets message details and if that succeedes calculates
    relevant events

    :param file_content:  (str) compelete Whatsapp log line
    :return: (list): list containing a dictionary for every log line
    [
        {
         'message_text':(str),
         'message_time':(datetime),
         'message_owner': (str),
         'is_leet':(int) 1 or 0,
         'is_zeet':(int) 1 or 0,
         'is_420':(int) 1 or 0,
         'is_soli': (int) 1 or 0,
         'is_fail': (int) 1 or 0,
        },...
    ]
    """
    data = list()
    for line in file_content.split('\n'):
        details = parse_details(line)
        if details is None or not all(details.values()):
            continue
        else:
            leet_features = build_leet_features(details)
            details.update(leet_features)
            details.update(split_time(details['message_time']))
            data.append(details)

    return data


def split_time(dt):
    return dict(year=dt.year,
                month=dt.month,
                day=dt.day,
                hour=dt.hour,
                minute=dt.minute)


if __name__ == '__main__':
    base_path = os.path.dirname(__file__)
    test_path = os.path.join(base_path, '../test/test_data/joan_whatsapp.txt')
    data = list()
    with open(test_path) as f:
        data = parse_file_to_leet(f.read())

    pd.DataFrame(data).to_csv('foo.csv', index=False)
