from flask import Flask, jsonify

app = Flask(__name__)


@app.route("/health", methods=['GET'])
def health_check():
    message = {'message': 'Servidor ON'}

    return jsonify(message)
