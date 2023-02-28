from datetime import datetime
import pandas as pd
from publisher import utils
from publisher.converter.post_parser.post_parser import PostParser
from publisher.influx.classification.classification_parser import ClassificationParser


class TemperatureOverlayParser:
    def __init__(self, data):
        self._raw_data = data
        self._data = data['data']
        self._identifier = data['identifier']
        self._h = data['headers']

    def _clear_NaT_rows(self, data):
        return data[pd.notnull(data['Time'])]

    def _parse_event(self, data):
        d = {**self._raw_data, 'data': data}
        return pd.DataFrame(ClassificationParser(d).state_change_event(),
                            columns=[self._h['time'], 'Event-Type', 'Startup-Type', self._h['t1']])

    def _filter_startup(self, data):
        return data.loc[data['Event-Type'] == 'Startup']

    def _filter_shutdown(self, data):
        return data.loc[data['Event-Type'] == 'Shutdown']
    
    def _parse_data(self, data, index_time, startup_type):
        parsed_data = []
        for item in data.to_dict(orient='records'):
            item['Seconds'] = (item['Event-Time'] - index_time).total_seconds()
            item['Label'] = f"{index_time.year}-{index_time.month}-{index_time.day} {startup_type} Start"
            item[self._h['time']] = item['Event-Time']
            del item['Event-Time']
            parsed_data.append(item)
        return parsed_data

    def _get_startup_time(self, index_time):
        f = index_time - pd.Timedelta(seconds=0)
        t = index_time + pd.Timedelta(seconds=21600)
        sliced_data = self._data.sort_index().loc[str(f):str(t)]
        for item in sliced_data.to_dict(orient='records'):
            if item[self._h['f1']] and float(item[self._h['f1']]) > 0:
                return item['Event-Time']
        return None

    def _parse_each_startup(self, data):
        parsed_data = []
        k = self._h['time']
        self._data['Event-Time'] = self._data[k]
        self._data = self._data.set_index(k)
        for item in data:
            start_time = self._get_startup_time(item[k])
            if not start_time:
                continue
            f = start_time - pd.Timedelta(seconds=7200)
            t = start_time + pd.Timedelta(seconds=21600)
            filtered_data = self._data.sort_index().loc[str(f):str(t)]
            parsed_data.extend(self._parse_data(filtered_data, start_time, item['Startup-Type']))
        return parsed_data
    
    def _parse_each_shutdown(self, data):
        parsed_data = []
        k = self._h['time']
        self._data['Event-Time'] = self._data[k]
        self._data = self._data.set_index(k)
        for item in data:
            start_time = self._get_startup_time(item[k])
            if not start_time:
                continue
            f = start_time - pd.Timedelta(seconds=7200)
            t = start_time + pd.Timedelta(seconds=21600)
            filtered_data = self._data.sort_index().loc[str(f):str(t)]
            parsed_data.extend(self._parse_data(filtered_data, start_time, item['Shutdown-Type']))
        return parsed_data

    def parse(self, args):
        self._data = self._clear_NaT_rows(self._data)
        self._data = self._data.astype(float, errors="ignore")
        event = self._parse_event(self._data.to_dict(orient='records'))
        startup = self._filter_startup(event)
        shutdown = self._filter_shutdown(event)
        startup_output = self._parse_each_startup(startup.to_dict(orient='records'))
        shutdown_output = self._parse_each_shutdown(shutdown.to_dict(orient='records'))
        startup_parsed = PostParser(self._raw_data).convert(startup_output)
        shutdown_parsed = PostParser(self._raw_data).convert(startup_output)
        startup_parsed.extend(shutdown_parsed)
        parsed = startup_parsed
        return [{"measurement": "e",
                 "tags": {**utils.get_tag_values(self._identifier), 'Startup Label': item['Label']},
                 "fields": utils.get_field_values(item), "time": utils.get_timestamp(item)} for item in parsed],
