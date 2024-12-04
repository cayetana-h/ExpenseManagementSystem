import logging
import azure.functions as func
import pymysql
import json
import datetime
from shared.dead_letter_queue import send_to_dead_letter_queue  # Import the DLQ helper function

# Database configuration
db_config = {
    "host": "iestlab0001.westeurope.cloudapp.azure.com",
    "user": "CC_A_2",
    "password": "HzbbpLgNtn9jYYVqtzimH1tdtcr0U0SY3v-rPy-slbQ",
    "database": "CC_A_2"
}

headers = {
    "Content-type": "application/json",
    "Access-Control-Allow-Origin": "*"
}

def get_user_id(username):
    """
    Retrieve userId based on the username.
    """
    try:
        connection = pymysql.connect(**db_config)
        cursor = connection.cursor()

        query = "SELECT id FROM Users WHERE username = %s"
        cursor.execute(query, (username,))
        result = cursor.fetchone()

        return result[0] if result else None

    except pymysql.Error as db_error:
        logging.error(f"Database error while fetching userId: {db_error}")
        raise
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'connection' in locals() and connection:
            connection.close()

def add_expense_to_db(user_id, amount, date, description, category_id):
    """
    Handles inserting the expense into the database.
    """
    try:
        connection = pymysql.connect(**db_config)
        cursor = connection.cursor()

        query = """
            INSERT INTO Expenses (userId, amount, date, description, categoryId)
            VALUES (%s, %s, %s, %s, %s)
        """
        cursor.execute(query, (user_id, amount, date, description, category_id))
        connection.commit()

        return cursor.rowcount == 1

    except pymysql.Error as db_error:
        logging.error(f"Database error: {db_error}")
        raise
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'connection' in locals() and connection:
            connection.close()

def main(req: func.HttpRequest) -> func.HttpResponse:
    """
    Main function to handle adding expenses, including validation and Dead Letter Queue logging.
    """
    logging.info("Processing AddExpense function.")

    try:
        req_body = req.get_json()
        username = req_body.get('username')
        amount = req_body.get('amount')
        date = req_body.get('date')
        description = req_body.get('description')
        category_id = req_body.get('categoryId')

        required_fields = ['username', 'amount', 'date', 'description', 'categoryId']
        missing_fields = [field for field in required_fields if not req_body.get(field)]

        if missing_fields:
            error_message = {
                "error": f"Missing required fields: {', '.join(missing_fields)}",
                "requestBody": req_body,
                "timestamp": datetime.datetime.utcnow().isoformat()
            }
            send_to_dead_letter_queue(error_message)

            return func.HttpResponse(
                json.dumps({"message": "Invalid request logged to Dead Letter Queue"}),
                status_code=400,
                headers=headers
            )

        try:
            amount = float(amount)  # Ensure amount is a number
            if amount < 0:
                raise ValueError("Amount cannot be negative.")
        except ValueError as ve:
            error_message = {
                "error": str(ve),
                "requestBody": req_body,
                "timestamp": datetime.datetime.utcnow().isoformat()
            }
            send_to_dead_letter_queue(error_message)

            return func.HttpResponse(
                json.dumps({"message": "Invalid amount logged to Dead Letter Queue"}),
                status_code=400,
                headers=headers
            )

        user_id = get_user_id(username)
        if not user_id:
            error_message = {
                "error": f"Invalid username: {username}",
                "requestBody": req_body,
                "timestamp": datetime.datetime.utcnow().isoformat()
            }
            send_to_dead_letter_queue(error_message)

            return func.HttpResponse(
                json.dumps({"message": "Invalid username logged to Dead Letter Queue"}),
                status_code=404,
                headers=headers
            )

        if add_expense_to_db(user_id, amount, date, description, category_id):
            return func.HttpResponse(
                json.dumps({"message": "Expense added successfully"}),
                status_code=200,
                headers=headers
            )
        else:
            raise Exception("Database insertion failed for unknown reasons.")

    except json.JSONDecodeError as json_error:
        error_message = {
            "error": "Invalid JSON format",
            "details": str(json_error),
            "timestamp": datetime.datetime.utcnow().isoformat()
        }
        send_to_dead_letter_queue(error_message)

        return func.HttpResponse(
            json.dumps({"message": "Invalid JSON format logged to Dead Letter Queue"}),
            status_code=400,
            headers=headers
        )

    except Exception as e:
        error_message = {
            "error": str(e),
            "requestBody": req_body if 'req_body' in locals() else None,
            "timestamp": datetime.datetime.utcnow().isoformat()
        }
        send_to_dead_letter_queue(error_message)

        return func.HttpResponse(
            json.dumps({"message": "An error occurred, logged to Dead Letter Queue"}),
            status_code=500,
            headers=headers
        )