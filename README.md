# Middleware AWS - Flask

API utilizada no meu projeto de TCC para fornecer um "meio de campo" entre a AWS e as outras aplicações envolvidas.

Essa API funciona em conjunto com o projeto [CSV2RDF](https://github.com/ivansalvadori/csv2rdf), mais especificamente o meu fork, que contém rota utilizada nessa API.

## Índice

- [Recursos](#recursos)
- [Requisitos](#requisitos)
- [Instalação](#instalação)
- [Licença](#licença)

## Recursos

- Chamar API CSV2RDF para converter um arquivo CSV em RDF a partir de ontologias.
- Gravar log de controle de processamento no dynamo
- Gravar RDF gerado no S3

## Requisitos

- Python 3.7 ou superior.
- Flask (instalado automaticamente como parte das dependências).

## Instalação

```bash
git clone https://github.com/Theusaopia/tcc2-flask-aws-api
cd tcc2-flask-aws-api
python -m venv .venv
#inicie o venv de acordo com seu terminal
pip install -r requirements.txt
flask --app main run # localhost:5000
```

## Licença

MIT
