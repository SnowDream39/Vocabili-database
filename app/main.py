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

app = FastAPI()

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
    return RedirectResponse(url="/docs")

# 挂载子路由
app.include_router(update_router)
app.include_router(select_router)
