import xml.etree.ElementTree as ET
from datetime import datetime
from typing import List, Optional, Set

from tableauserverclient.datetime_helpers import parse_datetime
from .connection_item import ConnectionItem
from .exceptions import UnpopulatedPropertyError
from .property_decorators import (
    property_not_nullable,
    property_is_boolean,
)

class VirtualConnectionItem(object):
    def __init__(self, name: Optional[str] = None):
        self._certified = None
        self._connections = None
        self._created_at = None
        self._data_quality_warnings = None
        self._has_extracts = None
        self._id = None
        self._name: Optional[str] = name       
        self._updated_at = None  
        self._webpage_url = None
        

    @property
    def created_at(self) -> Optional[datetime]:
        return self._created_at

    @property
    def certified(self) -> Optional[bool]:
        return self._certified

    @certified.setter
    @property_not_nullable
    @property_is_boolean
    def certified(self, value: Optional[bool]):
        self._certified = value

    @property
    def connections(self) -> Optional[List[ConnectionItem]]:
        if self._connections is None:
            error = "Virtual Connection item must be populated with connections first."
            raise UnpopulatedPropertyError(error)
        return self._connections()
    
    @property
    def dqws(self):
        if self._data_quality_warnings is None:
            error = "Virtual Connection item must be populated with dqws first."
            raise UnpopulatedPropertyError(error)
        return self._data_quality_warnings()

    @property
    def has_extracts(self) -> Optional[bool]:
        return self._has_extracts

    @property
    def id(self) -> Optional[str]:
        return self._id
    
    @property
    def name(self) -> Optional[str]:
        return self._name

    @name.setter
    def name(self, value: Optional[str]) -> None:
        self._name = value
    
    @property
    def updated_at(self) -> Optional[datetime]:
        return self._updated_at
    
    @property
    def webpage_url(self) -> Optional[str]:
        return self._webpage_url
    
    def _set_connections(self, connections):
        self._connections = connections

    def _set_data_quality_warnings(self, dqws):
        self._data_quality_warnings = dqws

    @classmethod
    def from_response(
        cls,
        resp: bytes,
        ns,
    ) -> List["VirtualConnectionItem"]:
        all_virtualconnection_items = list()
        parsed_response = ET.fromstring(resp)
        all_virtualconnection_xml = parsed_response.findall(".//t:virtualConnection", namespaces=ns)
        for virtualconnection_xml in all_virtualconnection_xml:
            all_virtualconnection_items.append(cls.from_xml(virtualconnection_xml, ns))
        return all_virtualconnection_items

    @classmethod
    def from_xml(cls, virtualconnection_xml, ns):
        virtualconnection_item = cls()
        virtualconnection_item._certified = str(virtualconnection_xml.get("isCertified", None)).lower() == "true"
        virtualconnection_item._created_at = parse_datetime(virtualconnection_xml.get("createdAt", None))
        virtualconnection_item._has_extracts = virtualconnection_xml.get("hasExtracts", None)
        virtualconnection_item._id = virtualconnection_xml.get("id", None)
        virtualconnection_item._name = virtualconnection_xml.get("name", None)   
        virtualconnection_item._updated_at = parse_datetime(virtualconnection_xml.get("updatedAt", None))  
        virtualconnection_item._webpage_url = virtualconnection_xml.get("webpageUrl", None)

        return virtualconnection_item