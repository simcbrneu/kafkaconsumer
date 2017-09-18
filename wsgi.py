from flask import Flask
from consumer import consumer
application = Flask(__name__)

@application.route("/")
def routine():
    consumer()
    return "Hello World!"

if __name__ == "__main__":
    application.run()
