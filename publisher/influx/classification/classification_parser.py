import pandas as pd
import pydash
from publisher import utils
from publisher.converter.post_parser.post_parser import PostParser


class ClassificationParser:
    def __init__(self, data):
        self._all_data = data
        self._data = data['data']
        self._classification = data['classification']
        self._identifier = data['identifier']
        self._h = data['headers']
        self._static_metadata= data['static_metadata']

    def _start_type(self, data):
        pressure = data[self._h['p1']]
        start_type = pydash.find(self._classification,
                                 lambda x: x['drumPressureMin'] <= pressure <= x['drumPressureMax'])
        return start_type['startupType'] if start_type else ""

    def _event(self, index, item):
        ct_load_threshold_val= self._static_metadata['4']['CT load Threshold']
        prev_status = 1 if float(self._data[index - 1][self._h['l1']]) > int(ct_load_threshold_val) else 0
        return {
            **self._data[index],
            'Event-Timestamp': self._data[index][self._h['time']],
            'Event-Type': 'Shutdown' if prev_status > item['Status'] else 'Startup',
            'Startup-Type': self._start_type(self._data[index]) if prev_status < item['Status'] else 'NA'
        } if item['Status'] != prev_status else None

    def _parse_each(self, item, i):
        ct_load_threshold_val= self._static_metadata['4']['CT load Threshold']
        item['Event-Timestamp'] = item['Event-Type'] = ""
        item['Status'] = 1 if float(item[self._h['l1']]) > int(ct_load_threshold_val) else 0
        return self._event(i, item) if i > 0 else None

    def state_change_event(self):
        event_occurrence = []
        for i, item in enumerate(self._data, start=0):
            event_occurrence.append(event) if (event := self._parse_each(item, i)) is not None else None
        return event_occurrence

    def _parse_event_time_difference(self, data):
        for i, item in enumerate(data):
            if i > 0:
                item['Time-between-events'] = (item[self._h['time']] - data[i - 1][self._h['time']]).total_seconds()
        return data

    def _parse_event(self):
        data = pd.DataFrame(self.state_change_event(),
                            columns=['Event-Timestamp', 'Event-Type', self._h['p1'],
                                     'Startup-Type'])
        data[self._h['time']] = pd.to_datetime(data['Event-Timestamp'], format="%Y-%m-%d %H:%M:%S")
        del data['Event-Timestamp']
        return self._parse_event_time_difference(data.to_dict(orient='records'))

    def _clear_NaT_rows(self, data):
        return data[pd.notnull(data['Time'])]

    def parse(self, args):
        data = self._clear_NaT_rows(self._data)
        data = data.astype(float, errors="ignore")
        self._data = data.to_dict(orient='records')
        output = self._parse_event()
        parsed = PostParser(self._all_data).convert(output)
        return [{"measurement": "e", "tags": utils.get_tag_values(self._identifier),
                 "fields": utils.get_field_values(item), "time": utils.get_timestamp(item)} for item in parsed],
