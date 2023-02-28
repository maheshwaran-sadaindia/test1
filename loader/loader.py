import asyncio
import os
import pathlib

import pandas as pd
import numpy as np
import constants as const
from loader.MetricSystemUnits import MetricSystemUnits
from loader.data_classification import Classification
from loader.data_groups import Groups
from loader.data_identifier import Identifier
from loader.data_template import Template
from loader.file_fetcher import FileFetcher
from loader.data_metadata import Metadata
from service.loghandler import LogHandler


class SensorDataLoader:
    def __init__(self, headers, identifier, bucket, application, usecase, valve="valve 1"):
        self._headers = headers
        self._id = identifier
        self._bucket = bucket
        self._app = application
        self._usecase = usecase
        self._valve = valve

    def _read_csv(self, filename, cv_loc):
        column_lists = list(self._headers.values())
        cols = set(column_lists)
        df = pd.read_csv(filename, usecols=lambda c: c in cols,
                         parse_dates=[self._headers['time']], low_memory=False, encoding='windows-1252')
        _units = df.iloc[0].to_dict()
        self._input_units = list(f"{k}_{v}" for k, v in _units.items())
        df[self._headers['time']] = pd.to_datetime(df[self._headers['time']], errors='coerce')
        df = df.dropna(subset=[self._headers['time']])
        df = df.sort_values(by=self._headers['time'])
        if self._usecase == '11':
            df2 = pd.read_csv(cv_loc, usecols=lambda c: c in cols,
                             parse_dates=[self._headers['time']], low_memory=False, encoding='windows-1252')
            df2 = df2.dropna(subset=[self._headers['time']])
            df2 = df2.sort_values(by=self._headers['time'])
            df = pd.merge(df, df2, how='inner', on = 'Time')
            self._input_units.append('cv_cv')
        return df

    def _get_static_metadata(self):
        return Metadata().get_static_metadata(self._id)

    def _get_metadata(self):
        if self._app == "Drum Level Control" or self._app == "Pump Recirculation":
            return Metadata().get_metadata_dlc_pump(self._id)
        else:
            return Metadata().get_metadata(self._id)

    def _get_identifier(self):
        return Identifier().get_identifier(self._id)

    def _get_classification(self):
        return Classification().get_classification()

    def _get_metric_system_units(self):
        if self._app == "Drum Level Control" or self._app == "Pump Recirculation":
            return MetricSystemUnits().get_metric_units_dlc_pump(self._app, self._usecase)
        else:
            return MetricSystemUnits().get_metric_units(self._app)

    def _initilize_valve_id(self):
        meta_data = self._get_metadata()
        if 'valveMetaData1' in meta_data.keys():
            if self._valve == 'valve 1':
                return 'valveMetaData1'
            elif self._valve == 'valve 2':
                return 'valveMetaData2'
        elif 'valveMetaData' in meta_data.keys():
            return 'valveMetaData'

    def _get_groups(self):
        if self._usecase == '10.1':
            return Groups().get_dynamic_groups(self._id, self._initilize_valve_id())
        return Groups().get_groups()

    def _get_template_filename(self):
        return Template().get_template(self._id)['uploadedFilename']

    def _clear_downloaded_file(self, filename):
        pathlib.Path(filename).unlink(missing_ok=True)

    def _load_data(self, filename, cv_loc):
        data = {
            'default_metadata': self._get_metadata(),
            'identifier': self._get_identifier(),
            'classification': self._get_classification(),
            'data': self._read_csv(filename, cv_loc),
            'bucket': self._bucket,
            'headers': self._headers,
            'metric_units': self._get_metric_system_units(),
            'input_units': self._input_units,
            'application': self._app,
            'use_case': self._usecase,
            'valve': self._valve,
            'file_name' : filename,
            'static_metadata':self._get_static_metadata(),
            **self._get_groups()
        }
        self._clear_downloaded_file(filename)
        if self._usecase == '11':
            self._clear_downloaded_file(cv_loc)
        return data

    def load(self):
        cv_filename = ""
        asyncio.run(LogHandler().warning("Loading metadata"))
        filename = self._get_template_filename()
        asyncio.run(LogHandler().warning("Read template filename"))
        FileFetcher(filename).download()
        if self._usecase == '11':
            cv_filename = self._valve + filename
            FileFetcher(cv_filename).download_cv()
        asyncio.run(LogHandler().warning("Downloaded blob from storage"))
        dl_loc = f'data/{filename}'
        cv_loc = f'data/{cv_filename}'
        return self._load_data(dl_loc, cv_loc)