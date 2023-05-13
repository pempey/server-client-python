from .endpoint import QuerysetEndpoint, api
from .exceptions import MissingRequiredFieldError
from .dqw_endpoint import _DataQualityWarningEndpoint
from tableauserverclient.server import RequestFactory
from tableauserverclient.models import VirtualConnectionItem, PaginationItem, ConnectionItem

import logging

from typing import List, Optional, TYPE_CHECKING, Tuple

if TYPE_CHECKING:
    from ..request_options import RequestOptions
    from ...server import Server


from tableauserverclient.helpers.logging import logger


class VirtualConnections(QuerysetEndpoint):
    def __init__(self, parent_srv: "Server") -> None:
        super(VirtualConnections, self).__init__(parent_srv)
        self._data_quality_warnings = _DataQualityWarningEndpoint(self.parent_srv, "virtualconnection")

    @property
    def baseurl(self) -> str:
        return "{0}/sites/{1}/virtualconnections".format(self.parent_srv.baseurl, self.parent_srv.site_id)

    # Get all virtual connections
    @api(version="3.18")
    def get(self, req_options: Optional["RequestOptions"] = None) -> Tuple[List[VirtualConnectionItem], PaginationItem]:
        logger.info("Querying all virtual connections on site")
        url = self.baseurl
        server_response = self.get_request(url, req_options)
        pagination_item = PaginationItem.from_response(server_response.content, self.parent_srv.namespace)
        all_metric_items = VirtualConnectionItem.from_response(server_response.content, self.parent_srv.namespace)
        return all_metric_items, pagination_item
    
    # Populate virtual connection item's connections
    @api(version="3.18")
    def populate_connections(self, virtualconnection_item: VirtualConnectionItem) -> None:
        if not virtualconnection_item.id:
            error = "Virtual Connection item missing ID. Datasource must be retrieved from server first."
            raise MissingRequiredFieldError(error)

        def connections_fetcher():
            return self._get_virtualconnection_connections(virtualconnection_item)

        virtualconnection_item._set_connections(connections_fetcher)
        logger.info("Populated connections for virtualconnection (ID: {0})".format(virtualconnection_item.id))

    def _get_virtualconnection_connections(self, virtualconnection_item, req_options=None):
        url = "{0}/{1}/connections".format(self.baseurl, virtualconnection_item.id)
        server_response = self.get_request(url, req_options)
        connections = ConnectionItem.from_response(server_response.content, self.parent_srv.namespace)
        return connections
    
    # Update virtual connection connections
    @api(version="3.18")
    def update_connection(
        self, virtualconnection_item: VirtualConnectionItem, connection_item: ConnectionItem
    ) -> Optional[ConnectionItem]:
        url = "{0}/{1}/connections/{2}".format(self.baseurl, virtualconnection_item.id, connection_item.id)

        update_req = RequestFactory.Connection.update_req(connection_item)
        server_response = self.put_request(url, update_req)
        connections = ConnectionItem.from_response(server_response.content, self.parent_srv.namespace)
        if not connections:
            return None

        if len(connections) > 1:
            logger.debug("Multiple connections returned ({0})".format(len(connections)))
        connection = list(filter(lambda x: x.id == connection_item.id, connections))[0]

        logger.info(
            "Updated virtualconnection item (ID: {0} & connection item {1}".format(virtualconnection_item.id, connection_item.id)
        )
        return connection
    
    @api(version="3.5")
    def populate_dqw(self, item):
        self._data_quality_warnings.populate(item)

    @api(version="3.5")
    def update_dqw(self, item, warning):
        return self._data_quality_warnings.update(item, warning)

    @api(version="3.5")
    def add_dqw(self, item, warning):
        return self._data_quality_warnings.add(item, warning)

    @api(version="3.5")
    def delete_dqw(self, item):
        self._data_quality_warnings.clear(item)

    