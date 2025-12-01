from fastapi import FastAPI, UploadFile, File, Form, HTTPException, APIRouter
from fastapi.responses import JSONResponse
import shutil
import os
from datetime import datetime
from app.utils.filename import extract_file_name, generate_board_file_path, generate_data_file_path, BoardIdentity, DataIdentity

UPLOAD_DIR = "/var/www/Vocabili-database/data"

router = APIRouter(prefix='/upload')

@router.post("")
async def upload_file(file: UploadFile = File(...)):
    if not file:
        raise HTTPException(400, "No file uploaded")
    if (file.filename):
        ext = os.path.splitext(file.filename)[1]
        filename = f"{os.path.splitext(file.filename)[0]}{ext}"
        
        file_info = extract_file_name(os.path.splitext(file.filename)[0])
        if (type(file_info) == BoardIdentity):
            save_path = generate_board_file_path(file_info.board, file_info.part, file_info.issue)
        elif (type(file_info) == DataIdentity):
            save_path = generate_data_file_path(file_info.date)
        else:
            raise HTTPException(400, "File error")
        

        # 保存文件
        with open(save_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)

        return JSONResponse({
            "url": save_path,
            "name": filename,
            "size": file.size if file.size else None
        })

    else:
        raise HTTPException(400, "File error")
