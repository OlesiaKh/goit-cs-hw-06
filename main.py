from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler
from multiprocessing import Process
import mimetypes
import json
import urllib.parse
import pathlib
import socket
import logging

from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

# Конфігурація MongoDB
db_uri = "mongodb://mongodb:27017"

# Налаштування для сокета
LOCAL_UDP_IP = '127.0.0.1'
LOCAL_UDP_PORT = 5000


def send_packet_to_socket(packet):
    udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    target_address = LOCAL_UDP_IP, 5000
    udp_socket.sendto(packet, target_address)
    udp_socket.close()


class CustomHttpHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        payload = self.rfile.read(int(self.headers['Content-Length']))
        send_packet_to_socket(payload)
        self.send_response(302)
        self.send_header('Location', '/')
        self.end_headers()

    def do_GET(self):
        parsed_url = urllib.parse.urlparse(self.path)
        match parsed_url.path:
            case '/':
                self.render_html_page('index.html')
            case '/message':
                self.render_html_page('message.html')
            case _:
                if pathlib.Path().joinpath(parsed_url.path[1:]).exists():
                    self.serve_static_file()
                else:
                    self.render_html_page('error.html', 404)

    def render_html_page(self, file_name, status_code=200):
        self.send_response(status_code)
        self.send_header('Content-type', 'text/html')
        self.end_headers()
        with open(file_name, 'rb') as file_content:
            self.wfile.write(file_content.read())

    def serve_static_file(self):
        self.send_response(200)
        mime_type = mimetypes.guess_type(self.path)
        if mime_type:
            self.send_header('Content-type', mime_type[0])
        else:
            self.send_header('Content-type', 'text/plain')
        self.end_headers()
        with open(f'.{self.path}', 'rb') as static_content:
            self.wfile.write(static_content.read())


def start_http_server(server_cls=HTTPServer, handler_cls=CustomHttpHandler):
    server_location = ('0.0.0.0', 3000)
    http_server = server_cls(server_location, handler_cls)
    logging.info(f'Server running at: {server_location}')

    try:
        http_server.serve_forever()
    except Exception as server_err:
        logging.error(f'Server error: {server_err}')
        http_server.server_close()


def store_message_in_db(data):
    # Підключення до MongoDB
    mongo_client = MongoClient(db_uri, server_api=ServerApi("1"))
    database = mongo_client.project_db
    parsed_data = urllib.parse.unquote_plus(data.decode())
    
    try:
        parsed_data = {key: value for key, value in [item.split('=') for item in parsed_data.split('&')]}
        parsed_data['timestamp'] = str(datetime.now())
        database.messages.insert_one(parsed_data)
    except ValueError as parse_err:
        logging.error(f'Error parsing data: {parse_err}')
    except Exception as db_err:
        logging.error(f'Error with database operation: {db_err}')
    finally:
        mongo_client.close()


def run_udp_socket_server(ip, port):
    udp_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    server_address = ip, port
    udp_socket.bind(server_address)
    
    try:
        while True:
            received_data, client_address = udp_socket.recvfrom(1024)
            store_message_in_db(received_data)
    except Exception as socket_err:
        logging.error(f'UDP Socket error: {socket_err}')
        logging.info('Shutting down the socket server.')
    finally:
        udp_socket.close()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(threadName)s %(message)s')

    http_process = Process(target=start_http_server, args=(HTTPServer, CustomHttpHandler))
    http_process.start()

    udp_process = Process(target=run_udp_socket_server, args=(LOCAL_UDP_IP, LOCAL_UDP_PORT))
    udp_process.start()
