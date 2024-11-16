import sys
import os
sys.path.append(os.path.abspath(os.path.dirname(__file__) + "/.."))
import logging
import json
import azure.functions as func
from azure.storage.blob import BlobServiceClient
from GUI.models import db, Expense, Budget
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker # Assuming Expense is already defined in models.py  # Assuming Expense is already defined in models.py


def set_budget(req: func.HttpRequest) -> func.HttpResponse:
    try:
        # Parse incoming JSON data
        req_body = req.get_json()

        month = req_body.get('month')
        amount = req_body.get('amount')

        if not all([month, amount]):
            return func.HttpResponse(
                json.dumps({"message": "Missing required fields: 'month' or 'amount'", "status": "error"}),
                status_code=400,
                mimetype="application/json"
            )

        # Check if budget already exists for the month
        budget = Budget.query.filter_by(month=month).first()

        if budget:
            budget.amount = amount  # Update existing budget
        else:
            budget = Budget(month=month, amount=amount)
            db.session.add(budget)

        db.session.commit()

        return func.HttpResponse(
            json.dumps({"message": "Budget set successfully", "status": "success"}),
            status_code=200,
            mimetype="application/json"
        )
    except Exception as e:
        logging.error(f"Error while setting budget: {str(e)}")
        return func.HttpResponse(
            json.dumps({"message": str(e), "status": "error"}),
            status_code=500,
            mimetype="application/json"
        )
