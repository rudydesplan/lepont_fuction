import logging
import azure.functions as func
import json
import os
import requests
import uuid

def trigger_databricks_job(id_to_delete):
    databricks_domain = os.getenv('DATABRICKS_DOMAIN')
    databricks_token = os.getenv('DATABRICKS_TOKEN')
    job_id = os.getenv('DATABRICKS_JOB_ID_DELETE')

    idempotency_token = str(uuid.uuid4())

    url = f'https://{databricks_domain}/api/2.1/jobs/run-now'

    headers = {
        'Authorization': f'Bearer {databricks_token}',
        'Content-Type': 'application/json'
    }

    payload = {
        'job_id': int(job_id),
        'idempotency_token': idempotency_token,
        'notebook_params': {
            'id_to_delete': id_to_delete  # Passing 'id_to_delete' parameter to the Databricks notebook
        }
    }

    response = requests.post(url, headers=headers, json=payload)
    return response.json()

def main_logic(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Azure HTTP trigger function processed a request to trigger a Databricks job.')

    try:
        req_body = req.get_json()
    except ValueError:
        return func.HttpResponse("Invalid JSON", status_code=400)

    id_to_delete = req_body.get('id_to_delete')

    if id_to_delete:
        trigger_response = trigger_databricks_job(id_to_delete)
        logging.info(f"Databricks job triggered with response: {trigger_response}")

        return func.HttpResponse(json.dumps(trigger_response), status_code=200)
    else:
        return func.HttpResponse("No 'id_to_delete' received", status_code=400)
