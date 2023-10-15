from flask import Flask, jsonify, request
import requests
import json

app = Flask(__name__)


@app.route("/health", methods=['GET'])
def health_check():
    message = {'message': 'Servidor ON'}

    return jsonify(message)


@app.route('/get_rdf_file', methods=['POST'])
def upload_and_request():

    csv_file = request.files['csv-file']
    mapping_file = request.files['mapping-file']
    ontology_file = request.files['ontology-file']

    csv_encode = request.form['csvEncode']
    csv_separator = request.form['csvSeparator']
    rdf_format = request.form['rdfFormat']
    rdf_encode = request.form['rdfEncode']
    ontology_format = request.form['ontologyFormat']

    csv2rdf_request_url = 'http://localhost:8080/converte'
    files = {
        'csv-file': (csv_file.filename, csv_file.read()),
        'mapping-file': (mapping_file.filename, mapping_file.read()),
        'ontology-file': (ontology_file.filename, ontology_file.read())
    }

    data = {
        'csvEncode': csv_encode,
        'csvSeparator': csv_separator,
        'rdfFormat': rdf_format,
        'rdfEncode': rdf_encode,
        'ontologyFormat': ontology_format
    }

    response = requests.post(csv2rdf_request_url, files=files, data=data)

    if response.status_code == 200:

        with open("teste.ntriples", 'w', encoding='utf-8') as file:
            file.write(response.text)

        return "ok", 200
    else:
        return 'Erro na requisição para a outra API', 500


if __name__ == '__main__':
    app.run(debug=True)
