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

def filter_expenses(req: func.HttpRequest) -> func.HttpResponse:
    try:
        selected_category = req.params.get('category')  # Get category from query string
        if selected_category:
            expenses = Expense.query.filter_by(category=selected_category).all()
        else:
            expenses = Expense.query.all()

        expenses_list = [
            {
                "id": expense.id,
                "amount": expense.amount,
                "category": expense.category,
                "date": expense.date,
                "description": expense.description
            }
            for expense in expenses
        ]

        return func.HttpResponse(
            json.dumps({"message": "Expenses retrieved successfully", "status": "success", "data": expenses_list}),
            status_code=200,
            mimetype="application/json"
        )
    except Exception as e:
        logging.error(f"Error while filtering expenses: {str(e)}")
        return func.HttpResponse(
            json.dumps({"message": str(e), "status": "error"}),
            status_code=500,
            mimetype="application/json"
        )
