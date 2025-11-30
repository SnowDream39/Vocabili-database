from fastapi import FastAPI, UploadFile, File, HTTPException, APIRouter
from fastapi.responses import JSONResponse
import shutil
import os
from datetime import datetime

UPLOAD_DIR = "/var/www/Vocabili-database/data"

router = APIRouter(prefix='/upload')

@router.post("")
async def upload_file(file: UploadFile = File(...)):
    if not file:
        raise HTTPException(400, "No file uploaded")

    ext = os.path.splitext(file.filename)[1]
    filename = f"{os.path.splitext(file.filename)[0]}_{int(datetime.now().timestamp())}{ext}"
    save_path = os.path.join(UPLOAD_DIR, filename)

    # 保存文件
    with open(save_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

    return JSONResponse({
        "url": f"/uploads/{filename}",
        "name": filename,
        "size": file.size if file.size else None
    })
