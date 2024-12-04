import logging
import azure.functions as func
import pymysql
import json
import datetime
from shared.dead_letter_queue import send_to_dead_letter_queue  # Import DLQ helper

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

def edit_expense(expense_id, user_id=None, amount=None, date=None, description=None, category_id=None, status=None):
    '''
    Update an expense record in the Expenses table.
    '''
    try:
        # Check if the amount is negative and handle it
        if amount is not None and amount < 0:
            error_message = f"Invalid amount: {amount}. Amount cannot be negative."
            logging.error(error_message)
            send_to_dead_letter_queue({
                "error": error_message,
                "expenseId": expense_id,
                "parameters": {
                    "userId": user_id,
                    "amount": amount,
                    "date": date,
                    "description": description,
                    "categoryId": category_id,
                    "status": status
                },
                "timestamp": datetime.datetime.utcnow().isoformat()
            })
            return False, "Invalid amount. Amount cannot be negative."

        connection = pymysql.connect(**db_config)
        cursor = connection.cursor()

        # Build the SQL query dynamically based on provided fields
        query = "UPDATE Expenses SET"
        updates = []
        params = []

        if user_id is not None:
            updates.append(" userId = %s")
            params.append(user_id)
        if amount is not None:
            updates.append(" amount = %s")
            params.append(amount)
        if date is not None:
            updates.append(" date = %s")
            params.append(date)
        if description is not None:
            updates.append(" description = %s")
            params.append(description)
        if category_id is not None:
            updates.append(" categoryId = %s")
            params.append(category_id)
        if status is not None:
            updates.append(" status = %s")
            params.append(status)

        if not updates:
            # No fields to update
            return False, "No fields provided for update."

        query += ",".join(updates) + " WHERE id = %s"
        params.append(expense_id)

        cursor.execute(query, tuple(params))
        connection.commit()

        if cursor.rowcount == 1:
            logging.info(f"Expense updated successfully: ID {expense_id}")
            return True, None
        else:
            logging.warning(f"No expense found with ID {expense_id}.")
            return False, "Expense not found."

    except pymysql.MySQLError as db_error:
        logging.error(f"MySQL error: {str(db_error)}")
        return False, str(db_error)

    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'connection' in locals() and connection:
            connection.close()

def main(req: func.HttpRequest) -> func.HttpResponse:
    '''
    This is the entry point for HTTP calls to our Edit Expense function.
    '''
    logging.info('EditExpense function processed a request.')

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

    # Extract fields
    expense_id = req_body.get('id')
    user_id = req_body.get('userId')
    amount = req_body.get('amount')
    date = req_body.get('date')
    description = req_body.get('description')
    category_id = req_body.get('categoryId')
    status = req_body.get('status')

    # Validate the required parameter
    if not expense_id:
        error_message = "Missing required field: id"
        logging.error(error_message)

        # Log to Dead Letter Queue
        invalid_request = {
            "error": error_message,
            "requestBody": req_body,
            "timestamp": datetime.datetime.utcnow().isoformat()
        }
        send_to_dead_letter_queue(invalid_request)

        return func.HttpResponse(
            json.dumps({"error": error_message}),
            status_code=400,
            headers=headers
        )

    # Call the edit_expense function
    try:
        success, error_message = edit_expense(expense_id, user_id, amount, date, description, category_id, status)
        if success:
            return func.HttpResponse(
                json.dumps({"message": "Expense updated successfully"}),
                status_code=200,
                headers=headers
            )
        else:
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
                status_code=404 if error_message == "Expense not found." else 400,
                headers=headers
            )
    except Exception as e:
        error_message = str(e)
        logging.error(f"Unexpected error: {error_message}")

        # Log to Dead Letter Queue
        invalid_request = {
            "error": error_message,
            "requestBody": req_body,
            "timestamp": datetime.datetime.utcnow().isoformat()
        }
        send_to_dead_letter_queue(invalid_request)

        return func.HttpResponse(
            json.dumps({"error": error_message}),
            status_code=500,
            headers=headers
        )