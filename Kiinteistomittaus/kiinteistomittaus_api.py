import argparse
import json
import sys

import dateutil.parser
import pandas as pd
import pytz
import requests
from fvhdms import (
    save_df, get_default_argumentparser, parse_args,
    dataframe_into_influxdb
)

USER_AGENT = 'https://github.com/ForumViriumHelsinki/IlmastoviisaatTaloyhtiot/Kiinteistomittaus api-client/0.0.1 Python/{}'.format(
    '.'.join([str(x) for x in list(sys.version_info)[:3]]))


def parse_kiinteistomittaus_args() -> argparse.Namespace:
    """Add Kaltiot related arguments into parser.

    :return: result of argparse.parse_args() (argparse.Namespace)
    """
    parser = get_default_argumentparser()
    parser.add_argument('-A', '--apikey', required=True, help='Kiinteistömittaus API key')
    parser.add_argument('-B', '--baseurl', required=True, help='Kiintömittaus API base URL')
    parser.add_argument('-G', '--guid', required=True, help='Device id')
    parser.add_argument('--period', default='day', nargs='?', choices=['latest', 'day', 'all'],
                        help='Download all listed measurements (comma separated), "all" for all')
    parser.add_argument('--savejson', help='Save response json to file "name"')
    args = parse_args(parser)
    return args


def get_data_from_api(args):
    headers = {
        'x-functions-key': args.apikey,
        'user-agent': USER_AGENT,
    }
    request_data = {"c": "getwatermeterdata", "p": [args.guid, args.period]}
    res = requests.post(args.baseurl, json=request_data, headers=headers)
    if args.savejson:
        with open(args.savejson, 'wt') as f:
            f.write(json.dumps(res.json(), indent=1))
    return res.json()


def parse_data(data: dict):
    if data is None:
        with open('data.json', 'rt') as f:
            data = json.loads(f.read())
    parsed_data = []
    for l in data['p']:
        ts_str, val = l.split(' = ')
        # Timestamps are in UTC
        ts = pytz.UTC.localize(dateutil.parser.parse(ts_str))
        # Values may contain ',' instead of '.' as decimal separator
        parsed_data.append([ts, float(val.replace(',', '.'))])
    return parsed_data


def data_to_df(args, data):
    index = []
    values = []
    ids = []
    for row in data:
        index.append(row[0])
        values.append(row[1])
        ids.append(args.guid)
    d = {
        'watermeter': values,
        'dev-id': ids,
    }
    df = pd.DataFrame(data=d, index=index)
    df.index.name = 'time'
    df = df.sort_index()
    return df


def main():
    args = parse_kiinteistomittaus_args()
    data = get_data_from_api(args)
    parsed_data = parse_data(data)
    df = data_to_df(args, parsed_data)
    df['cnt'] = df['watermeter'].diff()
    # Save raw cumulative data to a file
    save_df(args, df)
    # Save raw cumulative data into InfluxDB
    dataframe_into_influxdb(vars(args), df, tag_columns=['dev-id'])
    # Create and save hourly usage
    # TODO


if __name__ == '__main__':
    main()
