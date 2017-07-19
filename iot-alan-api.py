# Tony Cheng <tony.pig@gmail.com>
# Version : 0.1
# Licensed under the Apache License, Version 2.0

import datetime
import time
import influxdb
# import pdb

from influxdb import InfluxDBClient
from influxdb.exceptions import InfluxDBServerError

from flask import Flask, request
from flask_restful import Api, Resource, reqparse

api_bindip = '0.0.0.0'
api_port = 7879
api_debug = True

db_host = '88.88.88.1'
db_port = 8086
db_user = ''
db_password = ''
db_name = 'alan'
db_retry_write_time = 3

app = Flask(__name__)
api = Api(app)

parser = reqparse.RequestParser()
parser.add_argument('name', type=str, required=True)
parser.add_argument('task', type=int, required=True, help='need task infornmation')
parser.add_argument('stat', type=int, required=True, help='need stat information')


class Iot_MK1(Resource):
    def __init__(self):
        args = parser.parse_args()
        self.name = args['name']
        self.task = args['task']
        self.stat = args['stat']
        self.iot_msg = {
                    "name": self.name,
                    "task": self.task,
                    "stat": self.stat
                  }
        print "iot_msg type is:", type(self.iot_msg)

    def get(self):
        print "iot_msg in get func type is:", type(self.iot_msg)
        print "iot_msg in get func context is:", self.iot_msg
        return self.iot_msg

    def post(self):
        write_result = write_influxdb(client, **self.iot_msg)
        if write_result == 1:
            msg = "Write into DB successfully : ", self.iot_msg
            return msg, 201
        else:
            return "unsuccessful ! The stat may be abnormal", 422


def connect_db(host, port, user, password, dbname):
    try:
        client = InfluxDBClient(host, port, user, password)
        database = client.get_list_database()
        ''' client.get_list_database()
            e.g. [{u'name': u'_internal'}, {u'name': u'monasca'}]
            <type 'list'>
        '''
        db_exist = False
        for current_dbname in database:
            item = current_dbname
            if dbname in (item[u'name']):
                print("Database: %s is exist, switch database to %s") % (dbname, dbname)
                db_exist = True
                client.switch_database(dbname)
                print("DB connected")
                return client
        if not db_exist:
            print("DB is not exist, trying to create database.....")
            client.create_database(dbname)
            print("DB %s created, trying to switch database") % dbname
            client.switch_database(dbname)
            return client
    except influxdb.client.InfluxDBClientError as client_err:
        raise Exception(str(client_err))
    except InfluxDBServerError as server_err:
        raise Exception(str(server_err))


def write_influxdb(client, **kwargs):
    timestamp = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
    print "iot_msg in write_influxdb is:", kwargs
    name = kwargs['name']
    task = kwargs['task']
    stat = kwargs['stat']

    json_body1 = [
        {
            "measurement": name,
            "tags": {
                "task": int(task),
                "stat": int(stat)
            },
            "fields": {
                "task": int(task),
                "stat": int(stat)
            }
        }
    ]
    try:
        client.write_points(json_body1)
        print("Writing points: {0}".format(json_body1))
        return 1
    except InfluxDBServerError as e_influxdbserver_err:
        # pdb.set_trace()
        if '503' in e_influxdbserver_err.message:
            time.sleep(db_retry_write_time)
            print("Caught '503 Service Unavailable' exception, need to wait and retry again.....")
            client.write_points(json_body1)
        else:
            print("Error - Write points: {0}".format(json_body1))
            print("Unknow error")

api.add_resource(Iot_MK1, '/alan')

if __name__ == "__main__":
    client = connect_db(host=db_host, port=db_port, user=db_user, password=db_password, dbname=db_name)
    app.run(host=api_bindip, port=api_port, debug=api_debug)
