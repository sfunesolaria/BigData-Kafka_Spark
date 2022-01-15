from time import sleep
import socket
import json
from opensky_api import OpenSkyApi

HOST = 'localhost'  # hostname o IP address
PORT = 20068         # puerto socket server

api = OpenSkyApi()
states = api.get_states(bbox=(36.173357, 44.024422,-10.137019, 1.736138))

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((HOST, PORT))
s.listen(1)
while True:
    print('\nEscoltant per un client a',HOST , PORT)
    conn, addr = s.accept()
    print('\Connectat per', addr)
    try:
        while(True):
            v = {}
            for vuelo in states.states:
                v = {
                    'callsign':vuelo.callsign,
                    'country': vuelo.origin_country,
                    'longitude': vuelo.longitude,
                    'latitude': vuelo.latitude,
                    'velocity': vuelo.velocity,
                    'vertical_rate': vuelo.vertical_rate,
                }
                print(v)
                conn.send(json.dumps(v).encode('utf-8'))
                conn.send(b'\n')
            sleep(10)       
    except socket.error:
        print ('Error .\n\nClient desconnectat.\n')
conn.close()