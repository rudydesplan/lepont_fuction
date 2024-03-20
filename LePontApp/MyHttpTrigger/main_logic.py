import logging
import azure.functions as func
import json
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from datetime import datetime
import uuid
import os

def main_logic(req: func.HttpRequest) -> func.HttpResponse:
    # Configuration du service Blob Storage
    connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
    container_name = "landing"  # Nom du conteneur pour les fichiers d'arrivée
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    container_client = blob_service_client.get_container_client(container_name)

    try:
        # Récupérer les données JSON de la requête
        req_body = req.get_json()
    except ValueError:
        return func.HttpResponse("Invalid JSON", status_code=400)

    if req_body:
        logging.info("Received data: {}".format(req_body))
        
        # Obtenir la date et l'heure actuelle
        date_time_now = datetime.now()
        
        # Format de la date et heure pour le nommage des dossiers
        date_folder = date_time_now.strftime("%Y-%m-%d")
        
        # Générer un identifiant unique (UUID) pour le nom initial du blob
        unique_id = str(uuid.uuid4())
        
        # Format de la date et heure pour le nom du fichier, selon le format spécifié
        date_time_for_name = date_time_now.strftime("%Y_%m_%d_%H_%M_%S")
        
        # Créer un nom initial pour le Blob en utilisant l'UUID et la date de création
        blob_name = f"{date_folder}/formulaire_{date_time_for_name}_{unique_id}.json"
        
        # Créer le blob client pour vérifier l'existence du blob
        blob_client = container_client.get_blob_client(blob_name)

        # Tenter de récupérer les propriétés du blob pour voir s'il existe déjà
        try:
            blob_client.get_blob_properties()
            # Si aucune exception n'est levée, le blob existe déjà, donc générer un nouvel UUID
            unique_id = str(uuid.uuid4())  # Générer un nouvel UUID
            blob_name = f"{date_folder}/formulaire_{unique_id}_{date_time_for_name}.json"
            blob_client = container_client.get_blob_client(blob_name)
        except Exception as e:
            # Si une exception est levée, le blob n'existe probablement pas
            logging.info("Blob does not exist, proceeding with the original name.")

        # Ajouter les clés 'id' et 'date_creation' au dictionnaire req_body
        req_body['id'] = unique_id
        req_body['date_creation'] = date_time_for_name

        # Sérialiser les données en JSON pour le stockage
        serialized_data = json.dumps(req_body)

        # Uploader les données dans le blob
        blob_client.upload_blob(serialized_data, overwrite=True)

        return func.HttpResponse(serialized_data, status_code=200)
    else:
        return func.HttpResponse("No data received", status_code=400)
