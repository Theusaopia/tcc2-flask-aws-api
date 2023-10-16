from flask import Flask, jsonify, request
from flask_cors import CORS
from clients_aws.dynamo_client import DynamoClient
from clients_aws.s3_client import S3Client
import requests

app = Flask(__name__)
CORS(app)


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
    id_execucao = request.form['id-exec']

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

    salva_status_dynamo(id_execucao, "EM PROCESSAMENTO")

    response = requests.post(csv2rdf_request_url, files=files, data=data)

    if response.status_code == 200:
        salva_status_dynamo(id_execucao, "PROCESSADO")

        file_name = f"RDF_{id_execucao}.ntriples"

        with open(file_name, 'w', encoding='utf-8') as file:
            file.write(response.text)

        salva_rdf_s3(file_name)

        return "Arquivo convertido e salvo", 200
    else:
        salva_status_dynamo(id_execucao, "ERRO AO PROCESSAR")
        return 'Erro ao converter CSV para RDF', 500


def salva_status_dynamo(id_execucao, status):
    dynamo = DynamoClient()
    dynamo.insert_control_data(id_execucao, status)


def salva_rdf_s3(filename):
    s3 = S3Client()
    s3.save_rdf_to_bucket(filename)


if __name__ == '__main__':
    app.run(debug=True)
