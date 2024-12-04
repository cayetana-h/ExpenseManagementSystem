import logging
import azure.functions as func
import pymysql
import json
import datetime
from shared.dead_letter_queue import send_to_dead_letter_queue  # Import the Dead Letter Queue helper

# Database configuration
db_config = {
    "host": "iestlab0001.westeurope.cloudapp.azure.com",
    "user": "CC_A_2",
    "password": "HzbbpLgNtn9jYYVqtzimH1tdtcr0U0SY3v-rPy-slbQ",
    "database": "CC_A_2"
}

def get_user(username):
    """
    Check if a user exists in the Users table by username.
    Returns:
        dict: {"exists": True/False, "user": {"id": ..., "username": ...} or None}
    """
    try:
        # Establish a database connection
        connection = pymysql.connect(**db_config)
        cursor = connection.cursor(pymysql.cursors.DictCursor)

        # Query to check if the user exists
        query = "SELECT id, username FROM Users WHERE username = %s"
        cursor.execute(query, (username,))
        result = cursor.fetchone()

        return {"exists": bool(result), "user": result if result else None}

    except pymysql.MySQLError as db_error:
        logging.error(f"MySQL error: {str(db_error)}")
        raise db_error

    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'connection' in locals() and connection:
            connection.close()

def main(req: func.HttpRequest) -> func.HttpResponse:
    """
    HTTP trigger function to handle checking if a user exists.
    """
    logging.info("Python HTTP trigger function processed a request.")

    try:
        # Parse query parameters
        username = req.params.get("username")
        if not username:
            error_message = "Missing required parameter: username"
            logging.error(error_message)

            # Log to Dead Letter Queue
            invalid_request = {
                "error": error_message,
                "requestBody": req.params,
                "timestamp": datetime.datetime.utcnow().isoformat()
            }
            send_to_dead_letter_queue(invalid_request)

            return func.HttpResponse(
                json.dumps({"message": "Invalid request logged to Dead Letter Queue"}),
                status_code=400,
                mimetype="application/json"
            )

        # Call the get_user function
        result = get_user(username)

        # If user does not exist, return 404
        if not result["exists"]:
            return func.HttpResponse(
                json.dumps({"message": f"No user found with username '{username}'"}),
                status_code=404,
                mimetype="application/json"
            )

        # If user exists, return user data
        return func.HttpResponse(
            json.dumps({"exists": True, "user": result["user"]}),
            status_code=200,
            mimetype="application/json"
        )

    except pymysql.MySQLError as db_error:
        logging.error(f"Database error: {str(db_error)}")

        # Log to Dead Letter Queue
        invalid_request = {
            "error": f"MySQL error: {str(db_error)}",
            "parameters": req.params,
            "timestamp": datetime.datetime.utcnow().isoformat()
        }
        send_to_dead_letter_queue(invalid_request)

        return func.HttpResponse(
            json.dumps({"error": "Database error occurred."}),
            status_code=500,
            mimetype="application/json"
        )

    except Exception as e:
        logging.error(f"Unexpected error: {str(e)}")

        # Log to Dead Letter Queue
        invalid_request = {
            "error": f"Unexpected error: {str(e)}",
            "parameters": req.params,
            "timestamp": datetime.datetime.utcnow().isoformat()
        }
        send_to_dead_letter_queue(invalid_request)

        return func.HttpResponse(
            json.dumps({"error": "An unexpected error occurred."}),
            status_code=500,
            mimetype="application/json"
        )