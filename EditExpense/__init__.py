import sys
import os
sys.path.append(os.path.abspath(os.path.dirname(__file__) + "/.."))
import logging
import json
import azure.functions as func
from azure.storage.blob import BlobServiceClient
from GUI.models import db, Expense 
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker # Assuming Expense is already defined in models.py  # Assuming Expense is already defined in models.py

def edit_expense(req: func.HttpRequest) -> func.HttpResponse:
    try:
        expense_id = req.route_params.get('expense_id')  # Get expense_id from route
        expense = Expense.query.get(expense_id)

        if not expense:
            return func.HttpResponse(
                json.dumps({"message": "Expense not found", "status": "error"}),
                status_code=404,
                mimetype="application/json"
            )

        # Parse JSON data from the request body
        req_body = req.get_json()
        expense.amount = req_body.get('amount', expense.amount)
        expense.category = req_body.get('category', expense.category)
        expense.date = req_body.get('date', expense.date)
        expense.description = req_body.get('description', expense.description)

        db.session.commit()

        return func.HttpResponse(
            json.dumps({"message": "Expense updated successfully", "status": "success"}),
            status_code=200,
            mimetype="application/json"
        )
    except Exception as e:
        logging.error(f"Error while updating expense: {str(e)}")
        return func.HttpResponse(
            json.dumps({"message": str(e), "status": "error"}),
            status_code=500,
            mimetype="application/json"
        )
