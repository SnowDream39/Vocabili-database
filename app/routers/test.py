from fastapi import APIRouter, Query
from app.utils.filename import extract_file_name

router = APIRouter(prefix='/test', tags=['test'])

@router.get("/extract_filename")
def extract_filename(filename: str = Query(...)):
    return extract_file_name(filename)