import json
from flask import Flask, request

app = Flask(__name__)


@app.route("/", methods=["POST"])
def webhook():
    print("--- Webhook Received ---")
    if request.is_json:
        try:
            data = request.get_json()
            print("Parsed JSON Payload:")
            print(json.dumps(data))
        except Exception as e:
            print(f"Failed to parse JSON: {e}")
    else:
        print("⚠️ Not JSON")

    return "Webhook processed successfully", 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=9000, debug=True)
