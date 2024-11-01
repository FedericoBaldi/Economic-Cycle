# Welcome to Cloud Functions for Firebase for Python!
# To get started, simply uncomment the below code or create your own.
# Deploy with `firebase deploy`

from firebase_functions import https_fn
from firebase_admin import initialize_app, firestore
from firebase_admin import credentials
from firebase_functions import firestore_fn, https_fn, scheduler_fn
from datetime import datetime, timedelta
import firebase_functions as functions
from economic_cycle_firebase import calc_economy_status


cred = credentials.Certificate('./credentials.json')
app = initialize_app(cred)

@scheduler_fn.on_schedule(schedule="every day 05:00")
# @https_fn.on_request(cors=functions.options.CorsOptions(cors_origins="*", cors_methods=["get", "post"]), region="europe-west8")
def daily_task(event: scheduler_fn.ScheduledEvent) -> None:
    status = calc_economy_status()

    # Get the current date for document ID
    today = datetime.utcnow().strftime("%Y-%m-%d")

    # Create a Firestore client
    db = firestore.client()

    # Create a document reference with the current date as ID
    doc_ref = db.collection("economy_status").document(today)

    # Set the status data as the document content
    doc_ref.set({"status": status, "datetime": today})
    return https_fn.Response("Economy status saved successfully!")

@https_fn.on_request(cors=functions.options.CorsOptions(cors_origins="*", cors_methods=["get", "post"]), region="europe-west8")
def get_latest_status(req: https_fn.Request) -> https_fn.Response:
    db = firestore.client()
    # Get a reference to the latest document by ordering by date descending
    # TODO: Check if this is the last or the first document
    doc_ref = db.collection("economy_status").order_by("datetime", direction=firestore.Query.DESCENDING).limit(1).get()

    # Check if documents exist
    if not doc_ref:
        raise https_fn.HttpsError(code=https_fn.FunctionsErrorCode.NOT_FOUND,
                              message=("No status data found"))

    # Get the document data (assumes "status" is the field name)
    status_data = doc_ref[0].to_dict()["status"]
    return https_fn.Response(status_data)