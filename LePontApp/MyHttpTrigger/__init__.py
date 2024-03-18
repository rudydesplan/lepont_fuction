import logging
import azure.functions as func
from .main_logic import main_logic

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    return main_logic(req)
