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

# Define response headers
headers = {
    "Content-type": "application/json",
    "Access-Control-Allow-Origin": "*"
}

def add_user(username):
    '''
    Add a new user with a unique username to the Users table.
    '''
    try:
        # Establish the database connection
        connection = pymysql.connect(**db_config)
        cursor = connection.cursor()

        # Insert the user into the Users table
        cursor.execute("INSERT INTO Users (username) VALUES (%s)", (username,))
        connection.commit()

        logging.info(f"User added successfully with username: {username}")
        return {"message": "User added successfully"}

    except pymysql.IntegrityError:
        error_message = f"Username '{username}' already exists."
        logging.error(error_message)
        return {"error": error_message}

    except pymysql.MySQLError as db_error:
        error_message = f"MySQL error: {str(db_error)}"
        logging.error(error_message)
        raise db_error

    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'connection' in locals() and connection:
            connection.close()

def main(req: func.HttpRequest) -> func.HttpResponse:
    '''
    HTTP trigger function to handle adding a new user.
    '''
    logging.info("Python HTTP trigger function processed a request.")

    try:
        # Parse the request body
        req_body = req.get_json()
        username = req_body.get("username")

        # Validate input
        if not username:
            error_message = "Missing required field: username"
            logging.error(error_message)
            send_to_dead_letter_queue({
                "error": error_message,
                "parameters": req_body,
                "timestamp": datetime.datetime.utcnow().isoformat()
            })
            return func.HttpResponse(
                json.dumps({"error": error_message}),
                status_code=400,
                headers=headers
            )

        # Call the function to add a user
        result = add_user(username)

        if "error" in result:
            return func.HttpResponse(
                json.dumps(result),
                status_code=409,  # Conflict
                headers=headers
            )

        return func.HttpResponse(
            json.dumps(result),
            status_code=201,  # Created
            headers=headers
        )

    except Exception as e:
        error_message = f"Unexpected error: {str(e)}"
        logging.error(error_message)
        send_to_dead_letter_queue({
            "error": error_message,
            "parameters": req_body,
            "timestamp": datetime.datetime.utcnow().isoformat()
        })
        return func.HttpResponse(
            json.dumps({"error": error_message}),
            status_code=500,
            headers=headers
        )