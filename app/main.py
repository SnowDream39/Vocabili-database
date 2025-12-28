import sys
import asyncio

# 🔧 Windows 下必须加这一行，放在所有 import 的最前面！
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse
from app.config import settings
from app.routers import update, select, upload, test, edit, output, search
from app.stores import data_store

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
app.include_router(update.router)
app.include_router(select.router)
app.include_router(upload.router)
app.include_router(test.router)
app.include_router(edit.router)
app.include_router(output.router)
app.include_router(search.router)

# 设置生命周期事件

@asynccontextmanager
async def lifespan(app: FastAPI):
    asyncio.create_task(cleanup_worker(task_manager))
    yield
    await data_store.shutdown()

    