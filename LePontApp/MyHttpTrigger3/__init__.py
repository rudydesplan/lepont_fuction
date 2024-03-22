import azure.functions as func
from .main_logic import main_logic

def main(req: func.HttpRequest) -> func.HttpResponse:
    # Simply call the main logic from main_logic.py
    return main_logic(req)