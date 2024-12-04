import logging
import azure.functions as func
import pymysql
import json
import datetime
from shared.dead_letter_queue import send_to_dead_letter_queue
from azure.storage.blob import BlobServiceClient  # Added for Blob Storage
import os  # Added for getting connection string
import urllib.parse  # Added to parse blob URL

# Database configuration
db_config = {
    "host": "iestlab0001.westeurope.cloudapp.azure.com",
    "user": "CC_A_2",
    "password": "HzbbpLgNtn9jYYVqtzimH1tdtcr0U0SY3v-rPy-slbQ",
    "database": "CC_A_2"
}

# Define response headers
headers = {
    "Content-type": "application/json",
    "Access-Control-Allow-Origin": "*"
}

def delete_expense_and_receipt(expense_id):
    '''
    Delete an expense record and its associated receipt from the database and blob storage.
    '''
    try:
        # Establish the database connection
        connection = pymysql.connect(**db_config)
        cursor = connection.cursor(pymysql.cursors.DictCursor)

        # First, find and delete the associated receipt(s)
        cursor.execute("""
            SELECT fileUrl 
            FROM Receipts 
            WHERE expenseId = %s
        """, (expense_id,))
        
        receipts = cursor.fetchall()
        
        # Delete receipts from Blob Storage
        if receipts:
            try:
                # Get connection string from environment variable
                connect_str = os.environ.get('AzureWebJobsStorage')
                blob_service_client = BlobServiceClient.from_connection_string(connect_str)
                
                # Delete each receipt blob
                for receipt in receipts:
                    receipt_url = receipt['fileUrl']
                    
                    # Parse the URL to extract container and blob name
                    parsed_url = urllib.parse.urlparse(receipt_url)
                    path_parts = parsed_url.path.strip('/').split('/')
                    
                    # Assumes URL format: https://<account>.blob.core.windows.net/<container>/<blobname>
                    container_name = path_parts[0]
                    blob_name = '/'.join(path_parts[1:])
                    
                    # Get blob client and delete
                    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
                    blob_client.delete_blob()
                    logging.info(f"Receipts blob deleted: {receipt_url}")
                
                # Delete receipt records from Receipt table
                cursor.execute("""
                    DELETE FROM Receipts 
                    WHERE expenseId = %s
                """, (expense_id,))
                logging.info(f"Receipts records deleted for expense ID: {expense_id}")
            
            except Exception as blob_error:
                logging.error(f"Error deleting receipt blobs: {str(blob_error)}")
                # Continue with expense deletion even if blob deletion fails

        # Delete the expense from the Expenses table
        cursor.execute("""
            DELETE FROM Expenses
            WHERE id = %s
        """, (expense_id,))

        # Commit the transaction
        connection.commit()

        # Check if a row was deleted
        if cursor.rowcount == 1:
            logging.info(f"Expense deleted successfully: ID {expense_id}")
            return True
        else:
            logging.warning(f"No expense found with ID {expense_id}.")
            return False

    except pymysql.MySQLError as db_error:
        # Handle MySQL-specific errors
        logging.error(f"MySQL error: {str(db_error)}")
        raise db_error

    finally:
        # Close the database connection
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'connection' in locals() and connection:
            connection.close()

# The rest of the function remains the same as in the previous implementation
def main(req: func.HttpRequest) -> func.HttpResponse:
    '''
    This is the entry point for HTTP calls to our Delete Expense function.
    '''
    logging.info('Python HTTP trigger function processed a request.')

    # Get the expense ID from the request
    expense_id = req.params.get('id')

    # For HTTP DELETE requests, when params are provided in the HTTP body:
    if not expense_id:
        try:
            req_body = req.get_json()
        except ValueError:
            error_message = "Invalid input format, expected JSON"
            logging.error(error_message)

            # Log to Dead Letter Queue
            invalid_request = {
                "error": error_message,
                "requestBody": None,
                "timestamp": datetime.datetime.utcnow().isoformat()
            }
            send_to_dead_letter_queue(invalid_request)

            return func.HttpResponse(
                json.dumps({"error": error_message}),
                status_code=400,
                headers=headers
            )
        else:
            expense_id = req_body.get('id')

    # Validate input data
    if not expense_id:
        error_message = "Missing required field: id"
        logging.error(error_message)

        # Log to Dead Letter Queue
        invalid_request = {
            "error": error_message,
            "requestBody": req.params if req.params else req_body,
            "timestamp": datetime.datetime.utcnow().isoformat()
        }
        send_to_dead_letter_queue(invalid_request)

        return func.HttpResponse(
            json.dumps({"error": error_message}),
            status_code=400,
            headers=headers
        )

    try:
        # Ensure expense_id is an integer
        expense_id = int(expense_id)
    except ValueError:
        error_message = "Invalid ID format, must be an integer"
        logging.error(error_message)

        # Log to Dead Letter Queue
        invalid_request = {
            "error": error_message,
            "requestBody": req.params if req.params else req_body,
            "timestamp": datetime.datetime.utcnow().isoformat()
        }
        send_to_dead_letter_queue(invalid_request)

        return func.HttpResponse(
            json.dumps({"error": error_message}),
            status_code=400,
            headers=headers
        )

    # Call the function to delete the expense and receipt from the database and blob storage
    try:
        if delete_expense_and_receipt(expense_id):
            return func.HttpResponse(
                json.dumps({"message": "Expense and receipt deleted successfully"}),
                status_code=200,
                headers=headers
            )
        else:
            error_message = f"No expense found with the given ID: {expense_id}"
            logging.warning(error_message)

            # Log to Dead Letter Queue
            invalid_request = {
                "error": error_message,
                "expenseId": expense_id,
                "timestamp": datetime.datetime.utcnow().isoformat()
            }
            send_to_dead_letter_queue(invalid_request)

            return func.HttpResponse(
                json.dumps({"error": error_message}),
                status_code=404,
                headers=headers
            )
    except Exception as e:
        error_message = str(e)
        logging.error(f"Unexpected error: {error_message}")

        # Log to Dead Letter Queue
        invalid_request = {
            "error": error_message,
            "expenseId": expense_id,
            "timestamp": datetime.datetime.utcnow().isoformat()
        }
        send_to_dead_letter_queue(invalid_request)

        return func.HttpResponse(
            json.dumps({"error": error_message}),
            status_code=500,
            headers=headers
        )