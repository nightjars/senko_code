import LiveFilterDB
import flask
from flask_restful import Resource, Api, abort
import json
import flask_jsonpify
import Config

app = flask.Flask(__name__)
api = Api(app)

class Inversions(Resource):
    def get(self):
        db = LiveFilterDB.get_db()
        return {'inversions': LiveFilterDB.get_inversions(db)}

class Inversion(Resource):
    def get(self, inversion_id):
        db = LiveFilterDB.get_db()
        inversion = LiveFilterDB.get_inversions(db, id=inversion_id)
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
        db = LiveFilterDB.get_db()
        inversions = LiveFilterDB.get_inversions(db, brief=True)
        running = {}
        for run in Config.inversion_runs:
            running[run['id']] = True
        for inversion in inversions:
            inversion['active'] = True if inversion['id'] in running else False
        return inversions

    def put(self):
        if not flask.request.json:
            abort(400)
        req = flask.request.json
        if req['']


api.add_resource(Inversions, '/inversions')
api.add_resource(Inversion, '/inversions/<inversion_id>')
api.add_resource(Status, '/status')

if __name__ == "__main__":
    app.run(port='5002')