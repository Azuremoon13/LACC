from datetime import datetime
from flask import Flask, request, jsonify

class FixSizedDict(dict):
    def __init__(self, *args, maxlen=0, **kwargs):
        self._maxlen = maxlen
        super().__init__(*args, **kwargs)

    def __setitem__(self, key, value):
        dict.__setitem__(self, key, value)
        if self._maxlen > 0:
            if len(self) > self._maxlen:
                self.pop(next(iter(self)))

class ChatServer:
    def __init__(self):
        self.app = Flask(__name__)
        self.messages = {}
        self.add_routes()

# # Var for how long to store messages in Seconds
        self.msg_store_time = 60
# # Var to keep msgs after time expired but up to max
        self.keep_after_expired = False
# Var for how many messages to store per Cluster Key
        self.msg_amount = 300 

    def add_routes(self):
        self.app.add_url_rule('/v1/verify-connection', view_func=self.verify_connection, methods=['GET'])
        self.app.add_url_rule('/v1/get-chat', view_func=self.get_chat, methods=['GET'])
        self.app.add_url_rule('/v1/post-chat', view_func=self.post_chat, methods=['POST'])

    def verify_connection(self):
        return "", 200

    def get_chat(self):
        cluster_key = request.args.get('cluster_key')
        time_received = float(request.args.get('time_received', 0))

        if not time_received:
            return "", 400

        cluster_msgs = self.messages.get(cluster_key, {})
        response = {"messages": []}
        if cluster_msgs:
            for time in list(cluster_msgs.keys()):
                if time > time_received:
                    response["messages"].append(cluster_msgs[time])
                if datetime.now().timestamp() - time > self.msg_store_time and not self.keep_after_expired:
                    del cluster_msgs[time]

        return jsonify(response), 200

    def post_chat(self):
        cluster_key = request.args.get('cluster_key')
        post_data = request.json
        print(post_data)
        if not post_data:
            return "", 400

        time = datetime.now().timestamp()

        if cluster_key not in self.messages:
            self.messages[cluster_key] = FixSizedDict(maxlen=self.msg_amount)

        self.messages[cluster_key][time] = post_data

        return "", 200

    def run(self):
        self.app.run()

if __name__ == '__main__':
    server = ChatServer()
    server.run()