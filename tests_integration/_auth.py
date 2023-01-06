from dotenv import load_dotenv
load_dotenv()
import os


#class CLIENT:
#    CLIENT_ID = "1668de40-c3c0-452d-8862-b403e64ffc5b"
#    CLIENT_SECRET = "3H51_b-CH5nL3v.-dfHB2h0IxjPZG8emx."

class CLIENT:
    CLIENT_ID = os.environ.get("client_id")
    CLIENT_SECRET = os.environ.get("client_secret")
