import logging
import azure.functions as func
import json
import os
import requests
import uuid

def trigger_databricks_job(update_data):
    databricks_domain = os.getenv('DATABRICKS_DOMAIN')
    databricks_token = os.getenv('DATABRICKS_TOKEN')
    job_id = os.getenv('DATABRICKS_JOB_ID_UPDATE')  # Ensure this environment variable is correctly set in your Azure Function App settings

    idempotency_token = str(uuid.uuid4())

    url = f'https://{databricks_domain}/api/2.1/jobs/run-now'

    headers = {
        'Authorization': f'Bearer {databricks_token}',
        'Content-Type': 'application/json'
    }

    payload = {
        'job_id': int(job_id),
        'idempotency_token': idempotency_token,
        'notebook_params': update_data  # Passing the entire update_data as notebook parameters
    }

    response = requests.post(url, headers=headers, json=payload)
    return response.json()

def main_logic(req: func.HttpRequest) -> func.HttpResponse:  # Renamed to main for consistency
    logging.info('Azure HTTP trigger function processed a request to trigger a Databricks job for updating data.')

    try:
        update_data = req.get_json()  # This will contain the entire data set for the update
    except ValueError:
        return func.HttpResponse("Invalid JSON", status_code=400)

    # Check if 'feedbackId' is part of the update data, assuming 'feedbackId' is necessary for the update operation
    if 'feedbackId' in update_data:
        trigger_response = trigger_databricks_job(update_data)  # Pass the entire update_data dictionary
        logging.info(f"Databricks job triggered with response: {trigger_response}")

        return func.HttpResponse(json.dumps(trigger_response), status_code=200)
    else:
        return func.HttpResponse("Missing 'feedbackId' in the provided data", status_code=400)
