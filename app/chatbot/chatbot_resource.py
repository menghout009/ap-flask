from flask import Flask
from flask_restx import Resource, reqparse
from chatterbot import ChatBot
from chatterbot.trainers import ListTrainer
from chatterbot.response_selection import get_most_frequent_response
from app.extensions import api
import json
import os
import time

from cachetools import LRUCache
from cachetools.keys import hashkey
time.clock = time.time

app = Flask(__name__)


# Configuration
class Config:
    CONVERSATION_FILES = [
        "fag.json",
        "global_stock_most_active.json",
        "cambodia_gdp.json",
        "conversation.json",
        "local_exchange_rate.json",
        "global_crypto.json",
        "local_stock.json",

        # nary json file
        "cambodia_news.json",
        "global_raw_material.json",
        "local_raw_material.json",
        "global_stock.json",
        "local_knowledge.json",
        "history.json",
        "global_bond.json"
    ]


config = Config()



def load_json_data(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        data = json.load(file)
    return data


chatbot = ChatBot(
    name='DataVueBot',
    read_only=False,
    logic_adapters=["chatterbot.logic.BestMatch"],
    response_selection_method=get_most_frequent_response
)

# Create an LRU cache with a specified maximum size
cache = LRUCache(maxsize=1000)  # Adjust the size as needed
# chatbot = ChatBot('DataVueBot', response_selection_method=get_most_frequent_response)

conversation_data = []
for file_name in config.CONVERSATION_FILES:
    file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), file_name)
    conversation_data += load_json_data(file_path)

list_trainer = ListTrainer(chatbot)
for conversation_pair in conversation_data:
    list_trainer.train(conversation_pair)

chatbot_ns = api.namespace("api/v1/chatbot")
parser = reqparse.RequestParser()
parser.add_argument('user_message', type=str, required=True, help='User input text')


@chatbot_ns.route('/ask')
class ChatbotAsk(Resource):
    @chatbot_ns.expect(parser)
    def post(self):
        args = parser.parse_args()
        user_message = args['user_message']
        # Generate a cache key based on the user message
        cache_key = hashkey(user_message)
        # Attempt to retrieve the response from the cache
        response = cache.get(cache_key)

        if response is None:
            chatbot_response = chatbot.get_response(user_message)

            if not chatbot_response or chatbot_response.confidence < 0.5:
                return {'success': True,
                        'message: ': 'successfully',
                        'payload': "I apologize. I still learning"}

            else:
                response = chatbot_response.text
            cache[cache_key] = response

        return {'success': True,
                'message: ': 'successfully',
                'payload': response}
