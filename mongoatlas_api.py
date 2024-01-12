from flask import Flask, jsonify, request
from pymongo import MongoClient
from bson import ObjectId

app = Flask(__name__)

# MongoDB connection setup
client = MongoClient("mongodb+srv://satyamgour2409:satyamg2@mongodb-cluster.3t9jief.mongodb.net/?retryWrites=true&w=majority")
db = client['Mongo-kafka-project']
collection = db['delivery-data']


# Helper function to convert ObjectId to string
def jsonify_document(document):
    document['_id'] = str(document['_id'])
    return document


# API Endpoint to get all documents
@app.route('/api/documents', methods=['GET'])
def get_all_documents():
    documents = collection.find()
    serialized_documents = [jsonify_document(doc) for doc in documents]
    return jsonify({'documents': serialized_documents})


# API Endpoint to filter documents
@app.route('/api/documents/<field>/<value>', methods=['GET'])
def filter_documents(field, value):
    documents = collection.find({field: value})
    serialized_documents = [jsonify_document(doc) for doc in documents]
    return jsonify({'documents': serialized_documents})


# API Endpoint to perform aggregation
@app.route('/api/aggregate', methods=['GET'])
def aggregate_data():
    pipeline = [
        {"$group": {"_id": "$field_to_aggregate", "count": {"$sum": 1}}}
    ]
    result = list(collection.aggregate(pipeline))
    serialized_result = [jsonify_document(doc) for doc in result]
    return jsonify({'result': serialized_result})

if __name__ == '__main__':
    app.run(debug=True)
