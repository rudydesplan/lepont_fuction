import logging
import azure.functions as func
import json
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from datetime import datetime
import uuid
import os
import requests  # Assurez-vous que requests est inclus dans votre environnement Azure Function

def trigger_databricks_job(blob_path):
    databricks_domain = os.getenv('DATABRICKS_DOMAIN')
    databricks_token = os.getenv('DATABRICKS_TOKEN')
    job_id = os.getenv('DATABRICKS_JOB_ID')

    # URL de l'API pour déclencher un job
    url = f'https://{databricks_domain}/api/2.0/jobs/run-now'

    headers = {
        'Authorization': f'Bearer {databricks_token}',
        'Content-Type': 'application/json'
    }

    payload = {
        'job_id': job_id,
        'notebook_params': {
            'json_blob_path': blob_path  # Paramètre passé au notebook Databricks
        }
    }

    response = requests.post(url, headers=headers, json=payload)
    return response.json()

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Azure HTTP trigger function processed a request.')

    connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
    container_name = "landing"
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    container_client = blob_service_client.get_container_client(container_name)

    try:
        req_body = req.get_json()
    except ValueError:
        return func.HttpResponse("Invalid JSON", status_code=400)

    if req_body:
        date_time_now = datetime.now()
        date_folder = date_time_now.strftime("%Y-%m-%d")
        unique_id = str(uuid.uuid4())
        date_time_for_name = date_time_now.strftime("%Y_%m_%d_%H_%M_%S")
        blob_name = f"{date_folder}/formulaire_{date_time_for_name}_{unique_id}.json"

        blob_client = container_client.get_blob_client(blob_name)

        try:
            blob_client.get_blob_properties()
            unique_id = str(uuid.uuid4())
            blob_name = f"{date_folder}/formulaire_{unique_id}_{date_time_for_name}.json"
            blob_client = container_client.get_blob_client(blob_name)
        except Exception as e:
            logging.info("Blob does not exist, proceeding with the original name.")

        req_body['id'] = unique_id
        req_body['date_creation'] = date_time_for_name

        serialized_data = json.dumps(req_body)
        blob_client.upload_blob(serialized_data, overwrite=True)

        # Chemin complet du blob dans le conteneur
        blob_path = f"{container_name}/{blob_name}"
        trigger_response = trigger_databricks_job(f"dbfs:/mnt/{blob_path}")
        logging.info(f"Databricks job triggered: {trigger_response}")

        return func.HttpResponse(serialized_data, status_code=200)
    else:
        return func.HttpResponse("No data received", status_code=400)
