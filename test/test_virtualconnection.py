import os
import tempfile
import unittest
from io import BytesIO
from zipfile import ZipFile

import requests_mock
from defusedxml.ElementTree import fromstring

import tableauserverclient as TSC
from tableauserverclient.datetime_helpers import format_datetime
from tableauserverclient.server.endpoint.exceptions import InternalServerError
from tableauserverclient.server.endpoint.fileuploads_endpoint import Fileuploads
from tableauserverclient.server.request_factory import RequestFactory
from ._utils import read_xml_asset, read_xml_assets, asset


GET_XML = "virtualconnection_get.xml"
GET_EMPTY_XML = "virtualconnection_get_empty.xml"
POPULATE_CONNECTIONS_XML = "virtualconnections_populate_connections.xml"
UPDATE_CONNECTION_XML = "virtualconnection_connection_update.xml"


class VirtualConnectionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.server = TSC.Server("http://test", False)

        # Fake signin
        self.server._site_id = "dad65087-b08b-4603-af4e-2887b8aafc67"
        self.server._auth_token = "j80k54ll2lfMZ0tv97mlPvvSCRyD0DOM"
        self.server.version = "3.18"

        self.baseurl = self.server.virtualconnections.baseurl

    def test_get(self) -> None:
        response_xml = read_xml_asset(GET_XML)
        with requests_mock.mock() as m:
            m.get(self.baseurl, text=response_xml)
            all_virtualconnections, pagination_item = self.server.virtualconnections.get()

        self.assertEqual(2, pagination_item.total_available)
        self.assertEqual("e76a1461-3b1d-4588-bf1b-17551a879ad9", all_virtualconnections[0].id)
        self.assertEqual("2016-08-11T21:22:40Z", format_datetime(all_virtualconnections[0].created_at))
        self.assertEqual("2016-08-11T21:34:17Z", format_datetime(all_virtualconnections[0].updated_at))
        self.assertEqual("SampleVC", all_virtualconnections[0].name)
        self.assertEqual("https://web.com", all_virtualconnections[0].webpage_url)
        self.assertTrue(all_virtualconnections[0].has_extracts)
        self.assertTrue(all_virtualconnections[0].certified)

        self.assertEqual("9dbd2263-16b5-46e1-9c43-a76bb8ab65fb", all_virtualconnections[1].id)
        self.assertEqual("2016-08-04T21:31:55Z", format_datetime(all_virtualconnections[1].created_at))
        self.assertEqual("2016-08-04T21:31:55Z", format_datetime(all_virtualconnections[1].updated_at))
        self.assertEqual("Sample virtualconnection", all_virtualconnections[1].name)
        self.assertEqual("https://page.com", all_virtualconnections[1].webpage_url)
        self.assertTrue(all_virtualconnections[1].has_extracts)
        self.assertTrue(all_virtualconnections[1].certified)

    def test_get_before_signin(self) -> None:
        self.server._auth_token = None
        self.assertRaises(TSC.NotSignedInError, self.server.virtualconnections.get)

    def test_get_empty(self) -> None:
        response_xml = read_xml_asset(GET_EMPTY_XML)
        with requests_mock.mock() as m:
            m.get(self.baseurl, text=response_xml)
            all_virtualconnections, pagination_item = self.server.virtualconnections.get()

        self.assertEqual(0, pagination_item.total_available)
        self.assertEqual([], all_virtualconnections)

    def test_populate_connections(self) -> None:
        response_xml = read_xml_asset(POPULATE_CONNECTIONS_XML)
        with requests_mock.mock() as m:
            m.get(self.baseurl + "/9dbd2263-16b5-46e1-9c43-a76bb8ab65fb/connections?pageNumber=1&pageSize=100", text=response_xml)
            single_virtualconnection = TSC.VirtualConnectionItem("test") # "1d0304cd-3796-429f-b815-7258370b9b74",
            single_virtualconnection._id = "9dbd2263-16b5-46e1-9c43-a76bb8ab65fb"
            self.server.virtualconnections.populate_connections(single_virtualconnection)
            self.assertEqual("9dbd2263-16b5-46e1-9c43-a76bb8ab65fb", single_virtualconnection.id)
            connections = single_virtualconnection.connections

        self.assertTrue(connections)
        print(connections)
        ds1, ds2 = connections
        
        self.assertEqual("be786ae0-d2bf-4a4b-9b34-e2de8d2d4488", ds1.id)
        self.assertEqual("sqlserver", ds1.connection_type)
        self.assertEqual("forty-two.net", ds1.server_address)
        self.assertEqual("duo", ds1.username)
        
        
        self.assertEqual("970e24bc-e200-4841-a3e9-66e7d122d77e", ds2.id)
        self.assertEqual("snowflake", ds2.connection_type)
        self.assertEqual("database.com", ds2.server_address)
        self.assertEqual("heero", ds2.username)
        

    def test_update_connection(self) -> None:
        populate_xml, response_xml = read_xml_assets(POPULATE_CONNECTIONS_XML, UPDATE_CONNECTION_XML)

        with requests_mock.mock() as m:
            m.get(self.baseurl + "/be786ae0-d2bf-4a4b-9b34-e2de8d2d4488/connections", text=populate_xml)
            m.put(
                self.baseurl + "/be786ae0-d2bf-4a4b-9b34-e2de8d2d4488/connections/be786ae0-d2bf-4a4b-9b34-e2de8d2d4488",
                text=response_xml,
            )
            single_virtualconnection = TSC.VirtualConnectionItem("be786ae0-d2bf-4a4b-9b34-e2de8d2d4488")
            single_virtualconnection._id = "be786ae0-d2bf-4a4b-9b34-e2de8d2d4488"
            self.server.virtualconnections.populate_connections(single_virtualconnection)

            connection = list(single_virtualconnection.connections).pop()  # type: ignore[index]
            print(connection)
            connection.server_address = "bar"
            connection.server_port = "9876"
            connection.username = "foo"
            new_connection = self.server.virtualconnections.update_connection(single_virtualconnection, connection)
            self.assertEqual(connection.id, new_connection.id)
            self.assertEqual(connection.connection_type, new_connection.connection_type)
            self.assertEqual("bar", new_connection.server_address)
            self.assertEqual("9876", new_connection.server_port)
            self.assertEqual("foo", new_connection.username)
