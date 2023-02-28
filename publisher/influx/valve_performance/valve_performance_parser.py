import itertools
import pandas as pd
from publisher import utils
import numpy as np

from publisher.converter.post_parser.post_parser import PostParser
from publisher.influx.valve_performance.valve_range_parser import ValveRangeParser


class ValvePerformanceParser:
    def __init__(self, data):
        self._data = data['data']
        self._classification = data['classification']
        self._identifier = data['identifier']
        self._h = data['headers']
        self._metadata = data
        self._args = None
        self._bucket = data['bucket']
        self._static_metadata= data['static_metadata']
        if data['bucket'] == "use-case-6":
            self._valve_data =  data['static_metadata']['6']
        if data['bucket'] == "use-case-7":
            self._valve_data =  data['static_metadata']['7']

    def _populate_zeroed_values(self, item, i):
        item["zeroed_valve_demand"] = float(item[self._h['d1']]) if item[self._h['d1']] > int(self._valve_data['Valve Demand']) else 0.0
        item["zeroed_valve_feedback"] = float(item[self._h['f1']]) if item[self._h['f1']] > int(self._valve_data['Valve Feedback']) else 0.0

    def _populate_demand_feedback_delta_t(self, item, i):
        item['demand_feedback_delta_t'] = float(item["zeroed_valve_demand"] - item["zeroed_valve_feedback"])
        item['abs_demand_feedback_delta_t'] = float(abs(item['demand_feedback_delta_t']))

    def _populate_valve_position_change(self, item, i):
        if i > 0:
            item['value_position_change'] = self._data[i]['zeroed_valve_feedback'] - self._data[i - 1][
                'zeroed_valve_feedback']
        else:
            item['value_position_change'] = 0.0
        item['abs_value_position_change'] = abs(item['value_position_change'])

    def _populate_data_sample_time(self, item, i):
        item['data_sample_time_seconds'] = (self._data[1]['Time'] - self._data[0]['Time']).total_seconds()
        item['data_sample_time'] = (self._data[1]['Time'] - self._data[0]['Time']) / pd.Timedelta(minutes=1)

    def _populate_valve_stroke_speed(self, item, i):
        if not item['data_sample_time'] == 0:
            # item['valve_stroke_speed'] = abs(item['abs_value_position_change'] / 100) * self._args['stroke-width'] / \
            #                              item['data_sample_time']
            item['valve_stroke_speed'] = item['abs_value_position_change']/item['data_sample_time']
        else:
            item['valve_stroke_speed'] = 0.0

    def _populate_sign(self, item, i):
        if i == 0:
            item['sign'] = 1.0
        else:
            item['sign'] = np.sign(item['value_position_change'])

    def _populate_magnitude(self, item, i):
        if i == 0:
            item['magnitude'] = 0.0
        elif item['sign'] == self._data[i - 1]['sign']:
            item['magnitude'] = item['value_position_change'] + self._data[i - 1]['magnitude']
        else:
            item['magnitude'] = item['value_position_change']

    def _populate_count(self, item, i):
        if i > 0:
            if item['sign'] == self._data[i - 1]['sign']:
                prev = self._data[i - 1].get('count') if self._data[i - 1].get('count') is not None else 0
                item['count'] = prev + 1
            else:
                item['count'] = 1
        else:
            item['count'] = 1

    def _populate_cycle_magnitude(self, item, i):
        if i > 0:
            prev_val = self._data[i - 1]['count'] if self._data[i - 1]['count'] is not None else 0
            next_val = self._data[i + 1]['count'] if i + 1 < len(self._data) else 0
            item['cycle_magnitude'] = item['magnitude'] if item['count'] == max(prev_val, item['count'],
                                                                                next_val) else None
        else:
            item['cycle_magnitude'] = None
        item['abs_cycle_magnitude'] = abs(item['cycle_magnitude']) if item['cycle_magnitude'] is not None else None

    def _populate_cycle_avg_rate_change(self, item, i):
        if i > 0:
            prev_val = self._data[i - 1]['count'] if self._data[i - 1]['count'] is not None else 0
            next_val = self._data[i + 1]['count'] if i + 1 < len(self._data) else 0
            if item['count'] == max(prev_val, item['count'], next_val):
                if not item['data_sample_time'] == 0:
                    item['cycle_avg_rate_change'] = abs(item['magnitude'] / item['count']) / item['data_sample_time']
                else:
                    item['cycle_avg_rate_change'] = None
            else:
                item['cycle_avg_rate_change'] = None
        else:
            item['cycle_avg_rate_change'] = None

    def _populate_inch_cycle_avg_rate_change(self, item, i):
        if i > 0 and item['cycle_avg_rate_change'] is not None:
            item['inch_cycle_avg_rate_change'] = item['cycle_avg_rate_change'] / 100 * self._args['stroke-width']
        else:
            item['inch_cycle_avg_rate_change'] = None

    def _populate_max_rate_change(self, item, i):
        if i > 0:
            if item['inch_cycle_avg_rate_change'] == 0:
                item['max_rate_change'] = 0.0
            elif item['inch_cycle_avg_rate_change'] is not None:
                count = self._data[i - 1]['count']
                demand_differential = []
                for r in range(i - count, i + 1):
                    demand_differential.extend([
                        self._data[r]['demand_feedback_delta_t'],
                        self._data[r]['abs_demand_feedback_delta_t'],
                        self._data[r]['value_position_change'],
                        self._data[r]['abs_value_position_change'],
                    ]
                    )
                if demand_differential:
                    if not item['data_sample_time'] == 0:
                        item['max_rate_change'] = float(max(demand_differential) / item['data_sample_time'])
                    else:
                        item['max_rate_change'] = None
                else:
                    item['max_rate_change'] = None

            else:
                item['max_rate_change'] = None
        else:
            item['max_rate_change'] = None

    def _populate_inch_max_rate_change(self, item, i):
        if i > 0 and item['max_rate_change'] is not None:
            item['inch_max_rate_change'] = item['max_rate_change'] / 100 * self._args['stroke-width']
        else:
            item['inch_max_rate_change'] = None

    def _parse_each(self, filters):
        for i, item in enumerate(self._data, start=0):
            for fn in filters:
                getattr(self, fn)(item, i)
        return self._data

    def _clear_NaT_rows(self, data):
        return data[pd.notnull(data['Time'])]

    def parse(self, args):
        self._args = args
        data = self._clear_NaT_rows(self._data)
        data = data.astype(float, errors="ignore")
        self._data = data.to_dict(orient='records')
        diff_min = (self._data[1]['Time'] - self._data[0]['Time']) / pd.Timedelta(minutes=1)
        self._data = [dict(item, **{'Time-Diff-Minutes': diff_min}) for item in self._data]
        filters = [
            "_populate_zeroed_values",
            "_populate_demand_feedback_delta_t",
            "_populate_valve_position_change",
            "_populate_data_sample_time",
            "_populate_valve_stroke_speed",
            "_populate_sign",
            "_populate_magnitude",
            "_populate_count",
        ]
        magnitude_filters = [
            "_populate_cycle_magnitude",
            "_populate_cycle_avg_rate_change",
            "_populate_inch_cycle_avg_rate_change",
            "_populate_max_rate_change",
            "_populate_inch_max_rate_change",
        ]
        self._parse_each(filters)
        self._parse_each(magnitude_filters)
        grouped = ValveRangeParser(self._data, self._h, self._metadata).group()
        magnitude = [{"measurement": "e",
                      "tags": {**utils.get_tag_values(self._identifier), "group": "all"},
                      "fields": utils.get_field_values(item),
                      "time": utils.get_timestamp(item)} for item in self._data]
        valve_position = [{"measurement": "e",
                           "tags": {**utils.get_tag_values(self._identifier), "group": "valve-position"},
                           "fields": utils.get_field_values(item),
                           "time": utils.get_timestamp(item)} for item in grouped['valve-position']]

        demand_feedback = [{"measurement": "e",
                            "tags": {**utils.get_tag_values(self._identifier), "group": "demand-feedback"},
                            "fields": utils.get_field_values(item),
                            "time": utils.get_timestamp(item)} for item in grouped['demand-feedback']]

        stroke_speed = [{"measurement": "e",
                         "tags": {**utils.get_tag_values(self._identifier), "group": "stroke-speed"},
                         "fields": utils.get_field_values(item),
                         "time": utils.get_timestamp(item)} for item in grouped['stroke-speed']]

        magnitude_cycle = [{"measurement": "e",
                            "tags": {**utils.get_tag_values(self._identifier), "group": "magnitude-cycle"},
                            "fields": utils.get_field_values(item),
                            "time": utils.get_timestamp(item)} for item in grouped['magnitude-cycle']]

        return magnitude, valve_position, demand_feedback, stroke_speed, magnitude_cycle
