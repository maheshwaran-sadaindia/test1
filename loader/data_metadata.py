from db.execute import Execute
from loader.query import METADATA
from loader.query import DEFAULT_METADATA
from loader.query import METADATA_DLC_PUMP

class Metadata:
    def get_metadata(self, id):
        default_metadata = {}
        metadata = Execute(METADATA, (id,)).select()
        if not metadata:
            return default_metadata
        for m in metadata:
            if d := default_metadata.get(m['useCase']):
                default_metadata[m['useCase']] = {**d, m['name']: m['value']}
            else:
                default_metadata[m['useCase']] = {m['name']: m['value']}
        return default_metadata
    
    def get_static_metadata(self,id):
        default_metadata = {}
        metadata = Execute(DEFAULT_METADATA, (id,)).select()
        if not metadata:
            return default_metadata
        for m in metadata:
            if d := default_metadata.get(m['useCase']):
                default_metadata[m['useCase']] = {**d, m['name']: m['value']}
            else:
                default_metadata[m['useCase']] = {m['name']: m['value']}
        return default_metadata

    def get_metadata_dlc_pump(self, id):
        default_metadata = {}
        metadata = Execute(METADATA_DLC_PUMP, (id,)).select()
        if not metadata:
            return default_metadata
        for m in metadata:
            if d := default_metadata.get(m['type']):
                default_metadata[m['type']] = {**d, m['name']: m['value'], m['name']+'_unit': m['unit']}
            else:
                default_metadata[m['type']] = {m['name']: m['value'], m['name']+'_unit': m['unit']}
        return default_metadata

