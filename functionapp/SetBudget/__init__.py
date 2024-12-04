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

def get_user_id(username):
    '''
    Retrieve userId corresponding to a given username.
    '''
    try:
        connection = pymysql.connect(**db_config)
        cursor = connection.cursor()

        # Query to get userId
        cursor.execute("SELECT id FROM Users WHERE username = %s", (username,))
        result = cursor.fetchone()

        if result:
            return result[0]  # Return userId
        else:
            return None
    except pymysql.MySQLError as db_error:
        logging.error(f"MySQL error while fetching userId: {str(db_error)}")
        raise db_error
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'connection' in locals() and connection:
            connection.close()

def set_budget(user_id, category_id, budget_limit, start_date, end_date):
    '''
    Add or update a budget record in the Budgets table for a unique user/category combination.
    '''
    try:
        # Validate budget_limit is not negative
        try:
            budget_limit = float(budget_limit)  # Ensure it's a number
        except ValueError:
            return False, "invalid_limit"

        if budget_limit < 0:
            error_message = {
                "error": "Budget limit cannot be negative.",
                "parameters": {"userId": user_id, "categoryId": category_id, "budgetLimit": budget_limit},
                "timestamp": datetime.datetime.utcnow().isoformat()
            }
            send_to_dead_letter_queue(error_message)
            return False, "negative_limit"

        connection = pymysql.connect(**db_config)
        cursor = connection.cursor()

        # Check if a budget already exists for this user and category combination
        cursor.execute("""
            SELECT id FROM Budgets
            WHERE userId = %s AND categoryId = %s
        """, (user_id, category_id))
        existing_budget = cursor.fetchone()

        if existing_budget:
            error_message = {
                "error": f"Budget already exists for User ID {user_id} and Category ID {category_id}.",
                "parameters": {"userId": user_id, "categoryId": category_id},
                "timestamp": datetime.datetime.utcnow().isoformat()
            }
            send_to_dead_letter_queue(error_message)
            return False, "exists"

        # Insert a new record if no budget exists
        cursor.execute("""
            INSERT INTO Budgets (userId, categoryId, budget_limit, start_date, end_date)
            VALUES (%s, %s, %s, %s, %s)
        """, (user_id, category_id, budget_limit, start_date, end_date))
        action = "created"

        connection.commit()

        if cursor.rowcount > 0:
            logging.info(f"Budget {action} successfully: User ID {user_id}, Category ID {category_id}")
            return True, action
        else:
            logging.warning("No rows were affected in the database.")
            return False, None

    except pymysql.MySQLError as db_error:
        error_message = {
            "error": str(db_error),
            "parameters": {"userId": user_id, "categoryId": category_id},
            "timestamp": datetime.datetime.utcnow().isoformat()
        }
        send_to_dead_letter_queue(error_message)
        raise db_error
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'connection' in locals() and connection:
            connection.close()

def main(req: func.HttpRequest) -> func.HttpResponse:
    '''
    This is the entry point for HTTP calls to the Set Budget function.
    '''
    logging.info('Python HTTP trigger function processed a request.')

    try:
        req_body = req.get_json()
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

        # Get userId from username
        user_id = get_user_id(username)
        if not user_id:
            error_message = {
                "error": f"User with username '{username}' not found.",
                "parameters": req_body,
                "timestamp": datetime.datetime.utcnow().isoformat()
            }
            send_to_dead_letter_queue(error_message)
            return func.HttpResponse(
                json.dumps({"error": f"User with username '{username}' not found."}),
                status_code=404,
                headers=headers
            )

        # Call the function to set the budget
        success, action = set_budget(user_id, category_id, budget_limit, start_date, end_date)
        if success:
            return func.HttpResponse(
                json.dumps({"message": f"Budget {action} successfully."}),
                status_code=200,
                headers=headers
            )
        else:
            if action == "exists":
                return func.HttpResponse(
                    json.dumps({"message": f"Budget already exists for User '{username}' and Category ID {category_id}."}),
                    status_code=409,
                    headers=headers
                )
            elif action == "negative_limit":
                return func.HttpResponse(
                    json.dumps({"error": "Budget limit cannot be negative."}),
                    status_code=400,
                    headers=headers
                )
            return func.HttpResponse(
                json.dumps({"error": "Failed to set budget"}),
                status_code=500,
                headers=headers
            )

    except Exception as e:
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