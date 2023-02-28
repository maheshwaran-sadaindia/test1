import csv
import itertools
import logging
import math
import pandas as pd

import constants
from publisher import utils
from pyXSteam.XSteam import XSteam

from publisher.converter.post_parser.post_parser import PostParser
from publisher.influx.enthalpy.enthalpy_range_parser import EnthalpyRangeParser
from publisher.influx.output_field_units import OutputFieldUnits

NAN = 'nan'


class EnthalpyParserTurbine:
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

    def _v(self, item, key, to_absolute=False):
        return item[self._h[key]] + constants.atmospheric_pressure if to_absolute else item[self._h[key]]

    def _check_nan(self, item, key):
        return 0.0 if pd.isna(item[self._h[key]]) else item[self._h[key]]

    def _p(self, item):
        key = self._h['barometric-pressure']
        if key in item.keys():
            return constants.atmospheric_pressure if pd.isna(item[key]) else item[key]
        else:
            return constants.atmospheric_pressure

    def _populate_t_sat_inlet(self, item, i):
        t = self.steam_table.tsat_p(self._check_nan(item, 'steam-inlet-pressure') + self._p(item))
        item['t_sat_inlet'] = t if t is not NAN else None
        OutputFieldUnits().t_sat_inlet = "deg F"

    def _populate_t_sat_outlet(self, item, i):
        t = self.steam_table.tsat_p(self._check_nan(item, 'steam-outlet-pressure') + self._p(item))
        item['t_sat_outlet'] = t if t is not NAN else None
        OutputFieldUnits().t_sat_outlet = "deg F"

    def _populate_steam_inlet_enthalpy(self, item, i):
        if self._check_nan(item, 'steam-inlet-temp') < item['t_sat_inlet']:
            sin_temp = item['t_sat_inlet'] + 1
        else:
            sin_temp = self._check_nan(item, 'steam-inlet-temp')
        e = self._enthalpy((self._check_nan(item, 'steam-inlet-pressure') + self._p(item)), sin_temp)
        item['steam_inlet_enthalpy'] = e if e is not NAN else None
        OutputFieldUnits().steam_inlet_enthalpy = "Btu/lb"

    def _populate_steam_outlet_enthalpy(self, item, i):
        if self._check_nan(item, 'steam-outlet-temp') < item['t_sat_outlet']:
            sin_temp = item['t_sat_outlet'] + 1
        else:
            sin_temp = self._check_nan(item, 'steam-outlet-temp')
        e = self._enthalpy((self._check_nan(item, 'steam-outlet-pressure') + self._p(item)), sin_temp)
        item['steam_outlet_enthalpy'] = e if e is not NAN else None
        OutputFieldUnits().steam_outlet_enthalpy = "Btu/lb"

    def _populate_desp_set_enthalpy_calc(self, item, i):
        if not math.isnan(self._v(item, 'desup-set-enthalpy')):
            item['desup_set_enthalpy_calc'] = self._v(item, 'desup-set-enthalpy')
        else:
            if not math.isnan(self._v(item, 'desp-set-temp')):
                e = self._enthalpy((self._check_nan(item, 'steam-outlet-pressure') + self._p(item)),
                                   self._v(item, 'desp-set-temp'))
                item['desup_set_enthalpy_calc'] = e if e is not NAN else None
            else:
                item['desup_set_enthalpy_calc'] = None
        OutputFieldUnits().desup_set_enthalpy_calc = "Btu/lb"

    def _populate_water_inlet_enthalpy(self, item, i):
        e = self._enthalpy((self._check_nan(item, 'water-inlet-pressure') + self._p(item)),
                           self._check_nan(item, 'water-inlet-temp'))
        item['water_inlet_enthalpy'] = e if e is not NAN else None
        OutputFieldUnits().water_inlet_enthalpy = "Btu/lb"

    def _populate_water_vapor_pressure(self, item, i):
        p = self.steam_table.psat_t(self._check_nan(item, 'water-inlet-temp')) - self._p(item)
        item['water_vapor_pressure'] = p if p is not NAN else None
        OutputFieldUnits().water_vapor_pressure = "psi G"

    def _populate_water_steam_ratio(self, item, i):
        if self._check_nan(item, 'inlet-steam-mass-flow') != 0 and\
                not pd.isna(self._check_nan(item, 'inlet-steam-mass-flow')):
            item['water_steam_ratio'] = self._check_nan(item, 'water-mass-flow') / \
                                        self._check_nan(item, 'inlet-steam-mass-flow') * 100
        else:
            item['water_steam_ratio'] = 0.0

    def _populate_outlet_steam_superheat(self, item, i):
        if self._check_nan(item, 'steam-outlet-temp') - item['t_sat_outlet'] > 0:
            item['outlet_steam_superheat'] = self._check_nan(item, 'steam-outlet-temp') - item['t_sat_outlet']
        else:
            item['outlet_steam_superheat'] = 0.0
        OutputFieldUnits().outlet_steam_superheat = "deg F"

    def _populate_hb_water_flow(self, item, i):
        if item['steam_outlet_enthalpy'] >= item['steam_inlet_enthalpy'] or pd.isna(
                self._check_nan(item, 'inlet-steam-mass-flow')):
            item['hb_water_flow'] = 0.0
        else:
            n = item['steam_inlet_enthalpy'] - item['steam_outlet_enthalpy']
            d = item['steam_outlet_enthalpy'] - item['water_inlet_enthalpy']
            item['hb_water_flow'] = self._check_nan(item, 'inlet-steam-mass-flow') * (n / d)
        OutputFieldUnits().hb_water_flow = "kpph"

    def _populate_excess_water(self, item, i):
        item['excess_water'] = self._check_nan(item, 'water-mass-flow') - item['hb_water_flow']
        if self._check_nan(item, 'water-mass-flow') <= 0:
            item['excess_water_per'] = 0.0
        elif not item['hb_water_flow'] == 0:
            item['excess_water_per'] = item['excess_water'] / item['hb_water_flow'] * 100
        else:
            item['excess_water_per'] = self._check_nan(item, 'water-mass-flow')
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
            "_populate_t_sat_inlet",
            "_populate_t_sat_outlet",
            "_populate_steam_inlet_enthalpy",
            "_populate_steam_outlet_enthalpy",
            "_populate_desp_set_enthalpy_calc",
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
                                   "tags": {**utils.get_tag_values(self._identifier),
                                            "group": "outlet-steam-superheat"},
                                   "fields": utils.get_field_values(item),
                                   "time": utils.get_timestamp(item)} for item in grouped['outlet-steam-superheat']]

        excess_water_per = [{"measurement": "e",
                             "tags": {**utils.get_tag_values(self._identifier), "group": "excess-water-per"},
                             "fields": utils.get_field_values(item),
                             "time": utils.get_timestamp(item)} for item in grouped['excess-water-per']]

        return all_group, water_steam_ratio, outlet_steam_superheat, excess_water_per
