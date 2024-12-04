import logging
import azure.functions as func
import pymysql
import json
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

def update_budget(username, category_id, budget_limit, start_date, end_date):
    '''
    Update an existing budget record in the Budgets table for a unique username/category combination.
    '''
    try:
        # Validate and convert budget_limit
        try:
            budget_limit = float(budget_limit)  # Ensure it's a number
        except ValueError:
            error_message = {
                "error": "Invalid budget limit.",
                "parameters": {"username": username, "categoryId": category_id, "budgetLimit": budget_limit},
                "timestamp": datetime.datetime.utcnow().isoformat()
            }
            send_to_dead_letter_queue(error_message)
            return False, "invalid_limit"

        if budget_limit < 0:
            error_message = {
                "error": "Budget limit cannot be negative.",
                "parameters": {"username": username, "categoryId": category_id, "budgetLimit": budget_limit},
                "timestamp": datetime.datetime.utcnow().isoformat()
            }
            send_to_dead_letter_queue(error_message)
            return False, "negative_limit"

        # Establish the database connection
        connection = pymysql.connect(**db_config)
        cursor = connection.cursor()

        # Get userId from username
        cursor.execute("SELECT id FROM Users WHERE username = %s", (username,))
        user = cursor.fetchone()
        if not user:
            error_message = {
                "error": f"No user found with username '{username}'",
                "parameters": {"username": username, "categoryId": category_id},
                "timestamp": datetime.datetime.utcnow().isoformat()
            }
            send_to_dead_letter_queue(error_message)
            return False, "user_not_found"

        user_id = user[0]

        # Check if a budget exists for this user and category combination
        cursor.execute("""
            SELECT id FROM Budgets
            WHERE userId = %s AND categoryId = %s
        """, (user_id, category_id))
        existing_budget = cursor.fetchone()

        if existing_budget:
            # If the budget exists, update the record
            cursor.execute("""
                UPDATE Budgets
                SET budget_limit = %s, start_date = %s, end_date = %s
                WHERE id = %s
            """, (budget_limit, start_date, end_date, existing_budget[0]))
            action = "updated"
        else:
            # If no budget exists, log the error and send to DLQ
            error_message = {
                "error": f"No budget found for User '{username}' and Category ID {category_id}",
                "parameters": {"username": username, "categoryId": category_id},
                "timestamp": datetime.datetime.utcnow().isoformat()
            }
            send_to_dead_letter_queue(error_message)
            return False, "budget_not_found"

        # Commit the transaction
        connection.commit()

        # Check if the row was affected
        if cursor.rowcount > 0:
            logging.info(f"Budget {action} successfully: Username '{username}', Category ID {category_id}")
            return True, action
        else:
            logging.warning("No rows were affected in the database.")
            return False, None

    except pymysql.MySQLError as db_error:
        # Log and handle MySQL-specific errors
        error_message = {
            "error": str(db_error),
            "parameters": {"username": username, "categoryId": category_id},
            "timestamp": datetime.datetime.utcnow().isoformat()
        }
        send_to_dead_letter_queue(error_message)
        raise db_error

    finally:
        # Close the database connection
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'connection' in locals() and connection:
            connection.close()

def main(req: func.HttpRequest) -> func.HttpResponse:
    '''
    This is the entry point for HTTP calls to the Update Budget function.
    '''
    logging.info('Python HTTP trigger function processed a request.')

    try:
        # Parse the request body
        req_body = req.get_json()

        # Extract parameters from the request
        username = req_body.get('username')
        category_id = req_body.get('categoryId')
        budget_limit = req_body.get('budgetLimit')
        start_date = req_body.get('startDate')
        end_date = req_body.get('endDate')

        # Validate input data
        if not all([username, category_id, budget_limit, start_date, end_date]):
            error_message = {
                "error": "Missing required fields: username, categoryId, budgetLimit, startDate, endDate",
                "parameters": req_body,
                "timestamp": datetime.datetime.utcnow().isoformat()
            }
            send_to_dead_letter_queue(error_message)
            return func.HttpResponse(
                json.dumps({"error": "Missing required fields: username, categoryId, budgetLimit, startDate, endDate"}),
                status_code=400,
                headers=headers
            )

        # Validate that the budget limit is not negative
        try:
            budget_limit = float(budget_limit)  # Ensure it's a valid number
        except ValueError:
            error_message = {
                "error": "Invalid budget limit.",
                "parameters": req_body,
                "timestamp": datetime.datetime.utcnow().isoformat()
            }
            send_to_dead_letter_queue(error_message)
            return func.HttpResponse(
                json.dumps({"error": "Invalid budget limit."}),
                status_code=400,
                headers=headers
            )

        if budget_limit < 0:
            error_message = {
                "error": "Budget limit cannot be negative.",
                "parameters": req_body,
                "timestamp": datetime.datetime.utcnow().isoformat()
            }
            send_to_dead_letter_queue(error_message)
            return func.HttpResponse(
                json.dumps({"error": "Budget limit cannot be negative."}),
                status_code=400,
                headers=headers
            )

        # Call the function to update the budget
        success, action = update_budget(username, category_id, budget_limit, start_date, end_date)
        if success:
            return func.HttpResponse(
                json.dumps({"message": f"Budget {action} successfully."}),
                status_code=200,
                headers=headers
            )
        else:
            if action == "user_not_found":
                return func.HttpResponse(
                    json.dumps({"error": f"No user found with username '{username}'"}),
                    status_code=404,  # Not found HTTP status code
                    headers=headers
                )
            elif action == "budget_not_found":
                return func.HttpResponse(
                    json.dumps({"error": f"No budget found for Username '{username}' and Category ID {category_id}"}),
                    status_code=404,  # Not found HTTP status code
                    headers=headers
                )
            elif action == "negative_limit":
                return func.HttpResponse(
                    json.dumps({"error": "Budget limit cannot be negative."}),
                    status_code=400,
                    headers=headers
                )
            return func.HttpResponse(
                json.dumps({"error": "Failed to update budget"}),
                status_code=500,
                headers=headers
            )

    except Exception as e:
        # Log unexpected errors and send to DLQ
        error_message = {
            "error": str(e),
            "parameters": req_body if 'req_body' in locals() else {},
            "timestamp": datetime.datetime.utcnow().isoformat()
        }
        send_to_dead_letter_queue(error_message)
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            status_code=500,
            headers=headers
        )