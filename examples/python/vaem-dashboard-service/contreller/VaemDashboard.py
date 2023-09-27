from flask import Flask, jsonify
import sqlite3

from flask_cors import CORS
app = Flask(__name__)
CORS(app)

DATABASE = 'D:\\VAEM\\examples\\python\\src\\driver\\db\\vaem_dashboard.db'


def query_db(query, args=(), one=False):
    con = sqlite3.connect(DATABASE)
    cur = con.execute(query, args)
    rv = [dict((cur.description[idx][0], value)
               for idx, value in enumerate(row)) for row in cur.fetchall()]
    con.close()
    return (rv[0] if rv else None) if one else rv


@app.route('/valves', methods=['GET'])
def get_valves():
    valves = query_db("SELECT * FROM valve")
    return jsonify(valves)


@app.route('/vaem_status', methods=['GET'])
def get_vaem_status():
    statuses = query_db("SELECT * FROM vaem_status")
    return jsonify(statuses)


@app.route('/vaem_errors', methods=['GET'])
def get_vaem_errors():
    errors = query_db("SELECT * FROM vaem_errors")
    return jsonify(errors)


@app.route('/operation_log', methods=['GET'])
def get_operation_log():
    logs = query_db("SELECT * FROM operation_log")
    return jsonify(logs)


if __name__ == '__main__':
    app.run(debug=True)
