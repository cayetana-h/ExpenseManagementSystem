import logging
import azure.functions as func
import pymysql
import json
from decimal import Decimal
from datetime import date, datetime
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

def serialize_expenses(expenses):
    """
    Convert non-serializable types like Decimal, date, and datetime to serializable types.
    """
    for expense in expenses:
        for key, value in expense.items():
            # Convert Decimal to float
            if isinstance(value, Decimal):
                expense[key] = float(value)
            # Convert datetime or date to string
            elif isinstance(value, (datetime, date)):
                expense[key] = value.strftime('%Y-%m-%d')
    return expenses

def get_expenses(user_id, expense_id=None, category_id=None, start_date=None, end_date=None, status=None):
    '''
    Retrieve expense records from the Expenses table based on various filters.
    '''
    try:
        connection = pymysql.connect(**db_config)
        cursor = connection.cursor(pymysql.cursors.DictCursor)

        query = "SELECT * FROM Expenses WHERE userId = %s"
        params = [user_id]

        if expense_id:
            query += " AND id = %s"
            params.append(expense_id)
        if category_id:
            query += " AND categoryId = %s"
            params.append(category_id)
        if start_date:
            query += " AND date >= %s"
            params.append(start_date)
        if end_date:
            query += " AND date <= %s"
            params.append(end_date)
        if status:
            query += " AND status = %s"
            params.append(status)

        cursor.execute(query, tuple(params))
        result = cursor.fetchall()

        return serialize_expenses(result)

    except pymysql.MySQLError as db_error:
        logging.error(f"MySQL error: {str(db_error)}")
        raise db_error

    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'connection' in locals() and connection:
            connection.close()

def get_user_id(username):
    '''
    Retrieve userId from the Users table based on the username.
    '''
    try:
        connection = pymysql.connect(**db_config)
        cursor = connection.cursor(pymysql.cursors.DictCursor)

        query = "SELECT id FROM Users WHERE username = %s"
        cursor.execute(query, (username,))
        user = cursor.fetchone()

        if user:
            return user["id"]
        else:
            return None

    except pymysql.MySQLError as db_error:
        logging.error(f"MySQL error: {str(db_error)}")
        raise db_error

    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'connection' in locals() and connection:
            connection.close()

def main(req: func.HttpRequest) -> func.HttpResponse:
    '''
    This is the entry point for HTTP calls to the View Expenses function.
    '''
    logging.info('Python HTTP trigger function processed a request.')

    try:
        username = req.params.get('username')
        expense_id = req.params.get('expenseId')
        category_id = req.params.get('categoryId')
        start_date = req.params.get('startDate')
        end_date = req.params.get('endDate')
        status = req.params.get('status')

        # Validate username
        if not username:
            error_message = "Missing required field: username"
            logging.error(error_message)

            # Log invalid request to DLQ
            send_to_dead_letter_queue({
                "error": error_message,
                "parameters": {
                    "username": username,
                    "expenseId": expense_id,
                    "categoryId": category_id,
                    "startDate": start_date,
                    "endDate": end_date,
                    "status": status
                },
                "timestamp": datetime.utcnow().isoformat()
            })

            return func.HttpResponse(
                json.dumps({"error": error_message}),
                status_code=400,
                headers=headers
            )

        # Fetch userId from username
        user_id = get_user_id(username)
        if not user_id:
            return func.HttpResponse(
                json.dumps({"error": f"No user found with username '{username}'"}),
                status_code=404,
                headers=headers
            )

        # Fetch expenses
        expenses = get_expenses(user_id, expense_id, category_id, start_date, end_date, status)

        if expenses:
            # Wrap the `expenses` in a `data` key for DataTables compatibility
            return func.HttpResponse(
                json.dumps({"data": expenses}),
                status_code=200,
                headers=headers
            )
        else:
            return func.HttpResponse(
                json.dumps({"message": "No expenses found matching the criteria"}),
                status_code=404,
                headers=headers
            )

    except pymysql.MySQLError as e:
        logging.error(f"Database error: {str(e)}")

        # Log to Dead Letter Queue
        send_to_dead_letter_queue({
            "error": f"Database error: {str(e)}",
            "parameters": {},
            "timestamp": datetime.utcnow().isoformat()
        })

        return func.HttpResponse(
            json.dumps({"error": "Database connection failed or query failed"}),
            status_code=500,
            headers=headers
        )
    except Exception as e:
        logging.error(f"Unexpected error: {str(e)}")

        # Log to Dead Letter Queue
        send_to_dead_letter_queue({
            "error": str(e),
            "parameters": {},
            "timestamp": datetime.utcnow().isoformat()
        })

        return func.HttpResponse(
            json.dumps({"error": "An unexpected error occurred"}),
            status_code=500,
            headers=headers
        )