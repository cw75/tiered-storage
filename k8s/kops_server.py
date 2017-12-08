#!/usr/bin/env python3

from http.server import HTTPServer, BaseHTTPRequestHandler
import os

class KopsHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        if self.path == '/memory':
            print('Adding a new memory node...')
            if os.system('./add_node.sh m') == 0:
                self.send_response(200)
                self.wfile.write(bytes('Successfully added a memory node.', 'utf-8'))
            else:
                self.send_response(500)
                self.wfile.write(bytes('Unexpected error while adding nodes.', 'utf-8'))
        elif self.path == '/ebs':
            print('Addingta new EBS node...')
            if os.system('./add_node.sh e') == 0:
                self.send_response(200)
                self.wfile.write(bytes('Successfully added an EBS node.', 'utf-8'))
            else:
                self.send_response(500)
                self.wfile.write(bytes('Unexpected error while adding nodes.', 'utf-8'))
        else:
            self.send_response(404)
            self.wfile.write(bytes('Invalid path: ' + self.path, 'utf-8'))

def run():
    print('starting server...')

    server_address = ('', 80)
    httpd = HTTPServer(server_address, KopsHandler)
    print('running server...')
    httpd.serve_forever()


run()
