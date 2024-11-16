import sys
import os
sys.path.append(os.path.abspath(os.path.dirname(__file__) + "/.."))
import logging
import json
import azure.functions as func
from azure.storage.blob import BlobServiceClient
from GUI.models import db, Expense 
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


# Set up the SQLAlchemy engine and session
DATABASE_URI = (
    'mysql+mysqlconnector://CC_A_2:HzbbpLgNtn9jYYVqtzimH1tdtcr0U0SY3v-rPy-slbQ@'
    'iestlab0001.westeurope.cloudapp.azure.com:3306/CC_A_2'
)

# Create SQLAlchemy engine and session
engine = create_engine(DATABASE_URI)
Session = sessionmaker(bind=engine)
db_session = Session()

# Azure Blob Storage Configuration
AZURE_STORAGE_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=expensemanagement2447685;AccountKey=IO1chPeuvhcpfPpBWnB7NoSUt4AvQEVABCJe3kC3CX/PR/MpfB8gTbq1t1kEyRhFlmQ/Vj8QB0/s+AStiBoaxw==;EndpointSuffix=core.windows.net"
CONTAINER_NAME = "receipts"  # The name of your container where receipts will be stored

# Initialize Blob Service Client
blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
container_client = blob_service_client.get_container_client(CONTAINER_NAME)

# Function to handle POST request for adding an expense
def add_expense(req: func.HttpRequest) -> func.HttpResponse:
    try:
        # Parse form data
        req_body = req.form

        # Extract fields from the request
        amount = req_body.get('amount')
        category = req_body.get('category')
        date = req_body.get('date')
        description = req_body.get('description')
        receipt_file = req.files.get('receipt')  # File object

        if not all([amount, category, date]):
            return func.HttpResponse(
                json.dumps({"message": "Missing required fields: 'amount', 'category', or 'date'.", "status": "error"}),
                status_code=400,
                mimetype="application/json"
            )

        # Handle receipt upload to Azure Blob Storage
        receipt_path = None
        if receipt_file:
            file_name = f"{category}_{date}_{amount}.jpg"
            blob_client = container_client.get_blob_client(file_name)
            blob_client.upload_blob(receipt_file.read(), overwrite=True)  # Upload file bytes
            receipt_path = blob_client.url  # URL of the uploaded file

        # Save expense to the database
        new_expense = Expense(
            amount=amount,
            category=category,
            date=date,
            description=description,
            receipt_path=receipt_path
        )
        db_session.add(new_expense)
        db_session.commit()

        return func.HttpResponse(
            json.dumps({
                "message": "Expense added successfully",
                "expense": {
                    "amount": amount,
                    "category": category,
                    "date": date,
                    "description": description,
                    "receipt_path": receipt_path
                },
                "status": "success"
            }),
            status_code=200,
            mimetype="application/json"
        )

    except Exception as e:
        logging.error(f"Error while adding expense: {str(e)}")
        return func.HttpResponse(
            json.dumps({"message": str(e), "status": "error"}),
            status_code=500,
            mimetype="application/json"
        )