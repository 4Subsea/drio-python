import os

from dotenv import load_dotenv

load_dotenv()


class CLIENT:
    CLIENT_ID = os.environ.get("client_id")
    CLIENT_SECRET = os.environ.get("client_secret")
