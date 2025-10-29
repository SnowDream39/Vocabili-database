from sqlalchemy import create_engine, text

# 注意这里要用 psycopg3 前缀
engine = create_engine("postgresql+psycopg://vocawiki:vocawiki@localhost:5432/vocawiki", echo=True)

with engine.connect() as conn:
    result = conn.execute(text("SELECT 1"))
    print(result.scalar())  # 应该输出 1
