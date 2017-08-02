import LiveFilterDB
import flask
from flask_restful import Resource, Api, abort
import json
import flask_jsonpify
import Config
import logging

app = flask.Flask(__name__)
api = Api(app)

class Inversions(Resource):
    def get(self):
        db = LiveFilterDB.get_db()
        return {'inversions': LiveFilterDB.get_inversions(db)}

class Inversion(Resource):
    def get(self, inversion_id):
        db = LiveFilterDB.get_db()
        inversion = LiveFilterDB.get_inversions(db, id=int(inversion_id))
        if len(inversion):
            return inversion[0]
        else:
            abort(404, message='Inversion {} does not exist.'.format(inversion_id))

    def delete(self, inversion_id):
        pass

    def post(self, inversion_id):
        if not flask.request.json:
            abort(400)
        req = flask.request.json


class Status(Resource):
    def get(self):
        queue_manager = Config.queue_manager
        if queue_manager is None:
            return

        db = LiveFilterDB.get_db()
        inversions = LiveFilterDB.get_inversions(db, brief=True)
        running = {}
        for run in Config.inversion_runs:
            running[run['id']] = True
        for inversion in inversions:
            inversion['active'] = True if inversion['id'] in running else False
        return {'inversions': inversions,
                'newest_timestamp': Config.queue_manager.newest_data_timestamp,
                'current_timestamp': Config.queue_manager.last_sent_data_timestamp}

    def put(self):
        print("put")
        print (flask.request.json)
        if not flask.request.json:
            abort(400)
        req = flask.request.json
        change_to = req['active']
        found = False
        for run in Config.inversion_runs:
            if run['id'] == req['inversion']:
                found = True
                if not change_to:
                    Config.inversion_runs.remove(run)
        if not found and change_to:
            db = LiveFilterDB.get_db()
            inversion = LiveFilterDB.get_inversions(db, req['inversion'])
            if len(inversion):
                Config.add_inversion_run(inversion[0])


api.add_resource(Inversions, '/inversions')
api.add_resource(Inversion, '/inversion/<inversion_id>')
api.add_resource(Status, '/status')

if __name__ == "__main__":
    app.run(port='5002')