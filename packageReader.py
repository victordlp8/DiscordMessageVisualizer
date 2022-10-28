import csv
import json
from datetime import datetime

import attr
import pandas as pd
from tqdm import tqdm


@attr.s
class Paths():
    index = r'package\messages\index.json'
    data = r'package\messages\c{ID}\channel.json'
    messages = r'package\messages\c{ID}\messages.csv'


def main():
    paths = Paths()

    with open(paths.index, 'r') as f:
        index = json.load(f)
    del f

    messagesData, columns = {}, []
    for channelID, channelName in tqdm(index.items(), desc='Analyzing messages', unit='chat'):
        channelData = json.load(
            open(paths.data.format(ID=channelID), 'r', encoding='utf-8'))
        channelMessages = csv.reader(
            open(paths.messages.format(ID=channelID), 'r', encoding='utf-8'))

        try:
            name = f"{channelData['guild']['name']} | {channelData['name']}"
        except KeyError:
            try:
                if not channelName or channelData['name'] == None:
                    continue
                else:
                    name = channelData['name']
            except KeyError:
                name = channelName.replace('Direct Message with ', '').split('#')[0]

        dayCounter = {}
        for row in channelMessages:
            timestamp = row[1].split(' ')[0]
            if timestamp != 'Timestamp':
                day = datetime.strptime(timestamp, '%Y-%m-%d')
                columns.append(day)
                try:
                    dayCounter[day] += 1
                except KeyError:
                    dayCounter[day] = 1

        dayCounter_ordered = {}
        count = 0
        for day in sorted([day for day in dayCounter.keys()]):
            count += dayCounter[day]
            dayCounter_ordered[day.strftime('%d-%m-%Y')] = count

        messagesData[name] = dayCounter_ordered

    columns = list(set(columns))
    columns = sorted(columns)
    columns = [col.strftime('%d-%m-%Y') for col in columns]

    dt = pd.DataFrame.from_dict(messagesData, orient='index', columns=columns)
    dt.to_csv('messagesData.csv')


if __name__ == '__main__':
    main()
