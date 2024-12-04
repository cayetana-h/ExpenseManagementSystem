import logging
import pymysql
import json
import azure.functions as func
import datetime
import base64
import uuid
from azure.storage.blob import BlobServiceClient
from shared.dead_letter_queue import send_to_dead_letter_queue  # Import DLQ helper function

# Database configuration
db_config = {
    "host": "iestlab0001.westeurope.cloudapp.azure.com",
    "user": "CC_A_2",
    "password": "HzbbpLgNtn9jYYVqtzimH1tdtcr0U0SY3v-rPy-slbQ",
    "database": "CC_A_2"
}

# Response headers
headers = {
    "Content-type": "application/json",
    "Access-Control-Allow-Origin": "*"
}

# Allowed file types
ALLOWED_FILE_TYPES = ["pdf", "jpg", "jpeg", "png"]

# Azure Blob Storage configuration
blob_connection_string = "DefaultEndpointsProtocol=https;AccountName=expensemanagement777;AccountKey=ilrDVsmOETb3ki378IXyjEThRRAUYJLrwAQZVEstzxsAynp1I2YVszL25H4G9LvrNgMwEDRXnubA+ASt/cdmvQ==;EndpointSuffix=core.windows.net"
blob_container_name = "receipts"


def upload_to_blob(file_name, file_content):
    """
    Upload a file to Azure Blob Storage with a unique name.
    """
    try:
        unique_file_name = f"{uuid.uuid4()}_{file_name}"
        blob_service_client = BlobServiceClient.from_connection_string(blob_connection_string)
        blob_client = blob_service_client.get_blob_client(container=blob_container_name, blob=unique_file_name)

        # Upload the file
        blob_client.upload_blob(file_content, overwrite=False)
        logging.info(f"File uploaded successfully: {unique_file_name}")
        return True, f"https://{blob_service_client.account_name}.blob.core.windows.net/{blob_container_name}/{unique_file_name}"
    except Exception as e:
        logging.error(f"Error uploading file to Blob Storage: {str(e)}")
        return False, str(e)


def add_receipt(expense_id, file_name, file_url):
    """
    Add a new receipt entry to the Receipts table in the database.
    Ensures only one receipt per expenseId. Sends all errors to DLQ.
    """
    try:
        connection = pymysql.connect(**db_config)
        cursor = connection.cursor()

        # Check if a receipt already exists for the given expenseId
        cursor.execute("SELECT COUNT(*) FROM Receipts WHERE expenseId = %s", (expense_id,))
        result = cursor.fetchone()
        if result and result[0] > 0:
            error_message = f"Receipt already exists for Expense ID {expense_id}."
            logging.warning(error_message)
            send_to_dead_letter_queue({
                "error": error_message,
                "parameters": {"expenseId": expense_id},
                "timestamp": datetime.datetime.utcnow().isoformat(),
            })
            return False, error_message

        # Insert the new receipt
        cursor.execute(
            """
            INSERT INTO Receipts (expenseId, fileUrl, uploadDate)
            VALUES (%s, %s, %s)
            """,
            (expense_id, file_url, datetime.datetime.now()),
        )
        connection.commit()

        if cursor.rowcount > 0:
            logging.info(f"Receipt added successfully for Expense ID {expense_id}")
            return True, None
        else:
            error_message = f"No rows were affected when adding receipt for Expense ID {expense_id}."
            logging.warning(error_message)
            send_to_dead_letter_queue({
                "error": error_message,
                "parameters": {"expenseId": expense_id, "fileUrl": file_url},
                "timestamp": datetime.datetime.utcnow().isoformat(),
            })
            return False, error_message
    except pymysql.MySQLError as db_error:
        error_message = f"MySQL error: {str(db_error)}"
        logging.error(error_message)
        send_to_dead_letter_queue({
            "error": error_message,
            "parameters": {"expenseId": expense_id, "fileUrl": file_url},
            "timestamp": datetime.datetime.utcnow().isoformat(),
        })
        raise db_error
    finally:
        if "cursor" in locals() and cursor:
            cursor.close()
        if "connection" in locals() and connection:
            connection.close()


def main(req: func.HttpRequest) -> func.HttpResponse:
    """
    Azure Function entry point for handling receipt uploads.
    """
    logging.info("AddReceipt function processing a request.")

    try:
        # Parse the incoming JSON request
        req_body = req.get_json()
        expense_id = req_body.get("expenseId")
        file_name = req_body.get("fileName")
        file_content_base64 = req_body.get("fileContent")

        # Validate required fields
        if not expense_id or not file_name or not file_content_base64:
            error_message = "Missing required fields: expenseId, fileName, or fileContent."
            logging.error(error_message)

            # Send error to DLQ
            send_to_dead_letter_queue(
                {
                    "error": error_message,
                    "request": req_body,
                    "timestamp": str(datetime.datetime.utcnow()),
                }
            )

            return func.HttpResponse(
                json.dumps({"error": error_message}),
                status_code=400,
                headers=headers,
            )

        # Decode the base64 file content
        try:
            file_content = base64.b64decode(file_content_base64)
        except Exception as e:
            error_message = "Invalid file content: Base64 decoding failed."
            logging.error(error_message)

            # Send error to DLQ
            send_to_dead_letter_queue(
                {
                    "error": error_message,
                    "fileName": file_name,
                    "timestamp": str(datetime.datetime.utcnow()),
                }
            )

            return func.HttpResponse(
                json.dumps({"error": error_message}),
                status_code=400,
                headers=headers,
            )

        # Validate file type
        file_extension = file_name.split(".")[-1].lower()
        if file_extension not in ALLOWED_FILE_TYPES:
            error_message = f"Invalid file format: {file_extension}. Allowed formats are {', '.join(ALLOWED_FILE_TYPES)}"
            logging.warning(error_message)

            # Send error to DLQ
            send_to_dead_letter_queue(
                {
                    "error": error_message,
                    "fileName": file_name,
                    "timestamp": str(datetime.datetime.utcnow()),
                }
            )

            return func.HttpResponse(
                json.dumps({"error": error_message}),
                status_code=400,
                headers=headers,
            )

        # Upload file to Azure Blob Storage
        success, blob_url = upload_to_blob(file_name, file_content)
        if not success:
            error_message = f"Failed to upload file to Blob Storage: {blob_url}"
            logging.error(error_message)

            # Send error to DLQ
            send_to_dead_letter_queue(
                {
                    "error": error_message,
                    "fileName": file_name,
                    "timestamp": str(datetime.datetime.utcnow()),
                }
            )

            return func.HttpResponse(
                json.dumps({"error": error_message}),
                status_code=500,
                headers=headers,
            )

        # Add receipt to the database
        success, error_message = add_receipt(expense_id, file_name, blob_url)
        if success:
            return func.HttpResponse(
                json.dumps({"message": "Receipt uploaded successfully", "fileUrl": blob_url}),
                status_code=200,
                headers=headers,
            )
        else:
            send_to_dead_letter_queue({
                "error": error_message,
                "parameters": {"expenseId": expense_id, "fileName": file_name},
                "timestamp": datetime.datetime.utcnow().isoformat(),
            })
            return func.HttpResponse(
                json.dumps({"error": error_message}),
                status_code=400,
                headers=headers,
            )

    except Exception as e:
        error_message = f"Unexpected error: {str(e)}"
        logging.error(error_message)

        # Send error to DLQ
        send_to_dead_letter_queue(
            {
                "error": "Unexpected error",
                "details": str(e),
                "timestamp": str(datetime.datetime.utcnow()),
            }
        )

        return func.HttpResponse(
            json.dumps({"error": "Internal Server Error", "details": str(e)}),
            status_code=500,
            headers=headers,
        )