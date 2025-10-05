from flask import Blueprint, jsonify, request
import xmltodict
import json

from wso2.message_processor import MessageProcessor

bp = Blueprint('api', __name__, url_prefix='/api')

@bp.route('/xml_to_queue', methods=['POST'])
def parse_and_send():

    try:

        xml_payload = request.data
        json_obj = xmltodict.parse(xml_payload)
        json_payload = json.dumps(json_obj)

        producer = MessageProcessor(MessageProcessor.MessageProcessorType.SENDER)
        producer.send(json_payload)

        return jsonify({
            'status': 'OK',
        }), 200

    except Exception as ex:

        print(ex)

        return jsonify({
            'status': 'Internal server error',
        }), 500
