import logging
import azure.functions as func
import pymysql
import json
import decimal
import datetime
from shared.dead_letter_queue import send_to_dead_letter_queue  # DLQ helper import

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

def view_budget(username, category_id=None):
    '''
    Retrieve the budget record for a specific user (by username) and optionally by category.
    '''
    try:
        # Establish the database connection
        connection = pymysql.connect(**db_config)
        cursor = connection.cursor(pymysql.cursors.DictCursor)

        # Get userId from username
        cursor.execute("SELECT id FROM Users WHERE username = %s", (username,))
        user = cursor.fetchone()
        if not user:
            error_message = f"No user found with username '{username}'"
            logging.error(error_message)
            send_to_dead_letter_queue({
                "error": error_message,
                "parameters": {"username": username, "categoryId": category_id},
                "timestamp": datetime.datetime.utcnow().isoformat()
            })
            return None, "user_not_found"

        user_id = user["id"]

        # Query to fetch the budget
        query = "SELECT * FROM Budgets WHERE userId = %s"
        params = [user_id]

        # Add optional category filter
        if category_id:
            query += " AND categoryId = %s"
            params.append(category_id)

        # Execute the query
        cursor.execute(query, tuple(params))
        result = cursor.fetchall()  # Fetch all budgets matching the filters

        return result, None

    except pymysql.MySQLError as db_error:
        # Log the error and send to the Dead Letter Queue
        logging.error(f"MySQL error: {str(db_error)}")
        send_to_dead_letter_queue({
            "error": str(db_error),
            "parameters": {"username": username, "categoryId": category_id},
            "timestamp": datetime.datetime.utcnow().isoformat()
        })
        raise db_error

    finally:
        # Close the database connection
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'connection' in locals() and connection:
            connection.close()

def custom_json_serializer(obj):
    """
    Helper function to serialize Decimal and datetime types.
    """
    if isinstance(obj, decimal.Decimal):
        return float(obj)  # Convert Decimal to float
    elif isinstance(obj, datetime.date):
        return obj.isoformat()  # Convert date or datetime to ISO format string
    raise TypeError(f"Type {type(obj)} not serializable")

def main(req: func.HttpRequest) -> func.HttpResponse:
    '''
    This is the entry point for HTTP calls to the ViewBudget function.
    '''
    logging.info('Python HTTP trigger function processed a request.')

    # Extract username and optional categoryId from the query parameters
    username = req.params.get('username')
    category_id = req.params.get('categoryId')

    # Validate the input
    if not username:
        error_message = "Missing required field: username"
        logging.error(error_message)
        send_to_dead_letter_queue({
            "error": error_message,
            "parameters": {"username": username, "categoryId": category_id},
            "timestamp": datetime.datetime.utcnow().isoformat()
        })
        return func.HttpResponse(
            json.dumps({"error": error_message}),
            status_code=400,
            headers=headers
        )

    try:
        # Call the function to retrieve the budget
        budgets, error = view_budget(username, category_id)

        if error == "user_not_found":
            return func.HttpResponse(
                json.dumps({"error": f"No user found with username '{username}'"}),
                status_code=404,
                headers=headers
            )

        if budgets:
            # Serialize the response using the custom serializer
            return func.HttpResponse(
                json.dumps({"budgets": budgets}, default=custom_json_serializer),
                status_code=200,
                headers=headers
            )
        else:
            # Return a 404 if no budgets are found
            return func.HttpResponse(
                json.dumps({"message": "No budgets found for the specified username and category." if category_id else "No budgets found for the specified username."}),
                status_code=404,
                headers=headers
            )
    except Exception as e:
        # Handle unexpected errors and log to the Dead Letter Queue
        logging.error(f"Unexpected error: {str(e)}")
        send_to_dead_letter_queue({
            "error": str(e),
            "parameters": {"username": username, "categoryId": category_id},
            "timestamp": datetime.datetime.utcnow().isoformat()
        })
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            status_code=500,
            headers=headers
        )