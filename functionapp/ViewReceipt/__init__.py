import logging
import pymysql
import json
import azure.functions as func
from datetime import datetime
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

def get_receipt_url(expense_id):
    """
    Retrieve the file URL of a receipt from the database based on expenseId.
    """
    try:
        connection = pymysql.connect(**db_config)
        cursor = connection.cursor()

        # Query to fetch the file URL
        cursor.execute("SELECT fileUrl FROM Receipts WHERE expenseId = %s", (expense_id,))
        result = cursor.fetchone()

        if result:
            return result[0]  # Return the file URL
        else:
            return None

    except pymysql.MySQLError as db_error:
        logging.error(f"MySQL error: {str(db_error)}")
        raise db_error

    finally:
        if "cursor" in locals() and cursor:
            cursor.close()
        if "connection" in locals() and connection:
            connection.close()

def main(req: func.HttpRequest) -> func.HttpResponse:
    """
    Azure Function entry point for viewing receipts.
    """
    logging.info("ViewReceipt function processing a request.")

    try:
        # Parse the incoming request parameters
        expense_id = req.params.get("expenseId")

        # Validate the required parameter
        if not expense_id:
            error_message = "Missing required field: expenseId"
            logging.error(error_message)

            # Log the error to the Dead Letter Queue
            send_to_dead_letter_queue({
                "error": error_message,
                "parameters": {},
                "timestamp": datetime.utcnow().isoformat()
            })

            return func.HttpResponse(
                json.dumps({"error": error_message}),
                status_code=400,
                headers=headers
            )

        # Retrieve the receipt URL
        receipt_url = get_receipt_url(expense_id)

        if receipt_url:
            # Return the URL in a successful response
            return func.HttpResponse(
                json.dumps({"fileUrl": receipt_url}),
                status_code=200,
                headers=headers
            )
        else:
            error_message = f"No receipt found for Expense ID {expense_id}"
            logging.warning(error_message)

            # Log the error to the Dead Letter Queue
            send_to_dead_letter_queue({
                "error": error_message,
                "parameters": {"expenseId": expense_id},
                "timestamp": datetime.utcnow().isoformat()
            })

            return func.HttpResponse(
                json.dumps({"error": error_message}),
                status_code=404,
                headers=headers
            )

    except pymysql.MySQLError as db_error:
        logging.error(f"MySQL error: {str(db_error)}")
        return func.HttpResponse(
            json.dumps({"error": "Database error occurred."}),
            status_code=500,
            headers=headers
        )

    except Exception as e:
        logging.error(f"Unexpected error: {str(e)}")
        return func.HttpResponse(
            json.dumps({"error": "An unexpected error occurred."}),
            status_code=500,
            headers=headers
        )