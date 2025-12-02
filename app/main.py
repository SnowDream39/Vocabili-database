import sys
import asyncio

# 🔧 Windows 下必须加这一行，放在所有 import 的最前面！
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse
from app.config import settings
from app.routers.producer import router as producer_router
from app.routers.song import router as song_router
from app.routers.update import router as update_router
from app.routers.select import router as select_router
from app.routers.upload import router as upload_router
from app.routers.test import router as test_router
from app.routers.edit import router as edit_router
from app.routers.output import router as output_router


from app.utils.task import task_manager, cleanup_worker
app = FastAPI(root_path="/v2")

# 全局中间件
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.ALLOW_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/", include_in_schema=False)
async def root():
    return RedirectResponse(url="/v2/docs")

# 挂载子路由
app.include_router(update_router)
app.include_router(select_router)
app.include_router(upload_router)
app.include_router(test_router)
app.include_router(edit_router)
app.include_router(output_router)

# 设置自动任务
@app.on_event("startup")
async def start_cleanup():
    asyncio.create_task(cleanup_worker(task_manager))