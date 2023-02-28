import itertools
import pandas as pd

import constants
from publisher import utils
from pyXSteam.XSteam import XSteam

from publisher.converter.post_parser.post_parser import PostParser
from publisher.influx.enthalpy.enthalpy_range_parser import EnthalpyRangeParser
import logging

from publisher.influx.output_field_units import OutputFieldUnits

NAN = 'nan'


class EnthalpyParser:
    def __init__(self, data):
        self._data = data['data']
        self._classification = data['classification']
        self._identifier = data['identifier']
        self._h = data['headers']
        self._metadata = data
        self._args = None
        self.steam_table = XSteam(XSteam.UNIT_SYSTEM_FLS)
        self.steam_table.logger.setLevel(logging.ERROR)
        logging.getLogger("XSteam").setLevel(logging.ERROR)
        logging.getLogger("pyXSteam.RegionSelection").setLevel(logging.ERROR)

    def _enthalpy(self, pressure, temperature, precision=0):
        return self.steam_table.h_pt(pressure, temperature)

    def _p(self, item):
        key = self._h['barometric-pressure']
        if key in item.keys():
            return constants.atmospheric_pressure if pd.isna(item[key]) else item[key]
        else:
            return constants.atmospheric_pressure

    def _v(self, item, key, to_absolute=False):
        abs_pressure = self._p(item)
        return item[self._h[key]] + abs_pressure if to_absolute else item[self._h[key]]

    def _populate_t_sat_interstage(self, item, i):
        t = self.steam_table.tsat_p(self._v(item, 'steam-pressure', True))
        item['t_sat_interstage'] = t if t is not NAN else None
        OutputFieldUnits().t_sat_interstage = "deg F"

    def _populate_steam_inlet_enthalpy(self, item, i):
        t = item['t_sat_interstage'] + 1.0 \
            if item[self._h['steam-inlet-temp']] < item['t_sat_interstage'] else self._v(item, 'steam-inlet-temp')
        e = self._enthalpy(self._v(item, 'steam-pressure', True), t)
        item['steam_inlet_enthalpy'] = e if e is not NAN else None
        OutputFieldUnits().steam_inlet_enthalpy = "Btu/lb"

    def _populate_steam_outlet_enthalpy(self, item, i):
        t = item['t_sat_interstage'] + 1.0 \
            if item[self._h['steam-outlet-temp']] < item['t_sat_interstage'] else self._v(item, 'steam-outlet-temp')
        e = self._enthalpy(self._v(item, 'steam-pressure', True), t)
        item['steam_outlet_enthalpy'] = e if e is not NAN else None
        OutputFieldUnits().steam_outlet_enthalpy = "Btu/lb"

    def _populate_hrsg_outlet_enthalpy(self, item, i):
        e = self._enthalpy(self._v(item, 'outlet-pressure', True), self._v(item, 'hrsg-outlet-temp'))
        item['hrsg_outlet_enthalpy'] = e if e is not NAN else None
        OutputFieldUnits().hrsg_outlet_enthalpy = "Btu/lb"

    def _populate_hrsg_set_enthalpy(self, item, i):
        e = self._enthalpy(self._v(item, 'outlet-pressure', True), self._v(item, 'set-temp'))
        item['hrsg_set_enthalpy'] = e if e is not NAN else None
        OutputFieldUnits().hrsg_set_enthalpy = "Btu/lb"

    def _populate_water_inlet_enthalpy(self, item, i):
        e = self._enthalpy(self._v(item, 'water-inlet-pressure', True), self._v(item, 'water-inlet-temp'))
        item['water_inlet_enthalpy'] = e if e is not NAN else None
        OutputFieldUnits().water_inlet_enthalpy = "Btu/lb"

    def _populate_water_vapor_pressure(self, item, i):
        p = self.steam_table.psat_t(self._v(item, 'water-inlet-temp'))
        item['water_vapor_pressure'] = p - self._p(item) if p is not NAN else None
        OutputFieldUnits().water_vapor_pressure = "psi G"

    def _populate_water_steam_ratio(self, item, i):
        if not self._v(item, 'inlet-steam-mass-flow') == 0 and not pd.isna(self._v(item, 'inlet-steam-mass-flow')):
            item['water_steam_ratio'] = self._v(item, 'water-mass-flow') / self._v(item, 'inlet-steam-mass-flow') * 100
        else:
            item['water_steam_ratio'] = 0.0

    def _populate_outlet_steam_superheat(self, item, i):
        n = self._v(item, 'steam-outlet-temp') - item['t_sat_interstage']
        if n < 0:
            item['outlet_steam_superheat'] = 0.0
        else:
            item['outlet_steam_superheat'] = n
        OutputFieldUnits().outlet_steam_superheat = "deg F"

    def _populate_hb_water_flow(self, item, i):
        if item['steam_outlet_enthalpy'] >= item['steam_inlet_enthalpy']:
            item['hb_water_flow'] = 0.0
        else:
            n = item['steam_inlet_enthalpy'] - item['steam_outlet_enthalpy']
            d = item['steam_outlet_enthalpy'] - item['water_inlet_enthalpy']
            item['hb_water_flow'] = self._v(item, 'inlet-steam-mass-flow') * (n / d) \
                if not pd.isna(self._v(item, 'inlet-steam-mass-flow')) else 0.0
        OutputFieldUnits().hb_water_flow = "kpph"

    def _populate_excess_water(self, item, i):
        item['excess_water'] = self._v(item, 'water-mass-flow') - item['hb_water_flow']
        if self._v(item, 'water-mass-flow') <= 0:
            item['excess_water_per'] = 0.0
        else:
            if not item['hb_water_flow'] == 0:
                item['excess_water_per'] = item['excess_water'] / item['hb_water_flow'] * 100
            else:
                item['excess_water_per'] = self._v(item, 'water-mass-flow')
        OutputFieldUnits().excess_water = "kpph"

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
            "_populate_t_sat_interstage",
            "_populate_steam_inlet_enthalpy",
            "_populate_steam_outlet_enthalpy",
            "_populate_hrsg_outlet_enthalpy",
            "_populate_hrsg_set_enthalpy",
            "_populate_water_inlet_enthalpy",
            "_populate_water_vapor_pressure",
            "_populate_water_steam_ratio",
            "_populate_outlet_steam_superheat",
            "_populate_hb_water_flow",
            "_populate_excess_water"
        ]
        self._parse_each(filters)
        parsed = PostParser(self._metadata).convert(self._data)
        grouped = EnthalpyRangeParser(parsed, self._h, self._metadata).group()
        all_group = [{"measurement": "e",
                      "tags": {**utils.get_tag_values(self._identifier), "group": "all"},
                      "fields": utils.get_field_values(item),
                      "time": utils.get_timestamp(item)} for item in parsed]
        water_steam_ratio = [{"measurement": "e",
                              "tags": {**utils.get_tag_values(self._identifier), "group": "water-steam-ratio"},
                              "fields": utils.get_field_values(item),
                              "time": utils.get_timestamp(item)} for item in grouped['water-steam-ratio']]

        outlet_steam_superheat = [{"measurement": "e",
                                   "tags": {**utils.get_tag_values(self._identifier), "group": "outlet-steam-superheat"},
                                   "fields": utils.get_field_values(item),
                                   "time": utils.get_timestamp(item)} for item in grouped['outlet-steam-superheat']]

        excess_water_per = [{"measurement": "e",
                             "tags": {**utils.get_tag_values(self._identifier), "group": "excess-water-per"},
                             "fields": utils.get_field_values(item),
                             "time": utils.get_timestamp(item)} for item in grouped['excess-water-per']]
        return all_group, water_steam_ratio, outlet_steam_superheat, excess_water_per
