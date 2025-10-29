from datetime import date, timedelta
from dateutil.relativedelta import relativedelta
import pandas as pd
from fastapi import HTTPException

def generate_board_file_path(
    board: str,
    part: str,
    issue: int
) -> str:
    board_folder = {
        'vocaloid-daily': '日刊',
        'vocaloid-weekly': '周刊',
        'vocaloid-monthly': '月刊',
    }[board]
    part_folder = {
        'main': '总榜',
        'new': '新曲榜'
    }[part]
    if board == 'vocaloid-daily':
        end_date = date(2024,7,3) + timedelta(issue)
        start_date = end_date - timedelta(1)
        return f"./data/{board_folder}/{part_folder}/{'新曲榜' if part == 'new' else ''}{end_date.strftime('%Y%m%d')}与{start_date.strftime('%Y%m%d')}.xlsx"
    elif board == 'vocaloid-weekly':
        end_date = date(2024,8,31) + timedelta(issue*7)
        return f"./data/{board_folder}/{part_folder}/{'新曲榜' if part == 'new' else ''}{end_date.strftime('%Y-%m-%d')}.xlsx"
    elif board == 'vocaloid-weekly':
        month = date(2024,6,1) + relativedelta(months=issue)
        return f"./data/{board_folder}/{part_folder}/{'新曲榜' if part == 'new' else ''}{month.strftime('%Y-%m')}.xlsx"
    else:
        raise Exception('输入不符合条件')

        
def validate_excel(df: pd.DataFrame):
    df['__row__'] = df.index + 2
    errors: list[str] = []
    
    if 'title' in df.columns:
        df['title'] = df['title'].fillna('')
    for column_name in df.columns:
        invalid_title = df[df[column_name].isna()]
        if not invalid_title.empty:
            for _, row in invalid_title.iterrows():
                message = f"第 {row['__row__']} 行的 {column_name} 为空"
                print(message)
                errors.append(message)
    if len(errors) > 1:
        raise HTTPException(400, "\n".join(errors))
    
def read_excel(filepath: str) -> pd.DataFrame:
    """
    读取是标准的数据文件或排名文件。对常用字段进行预处理。
    """
    df = pd.read_excel(filepath, dtype={
        'title': str,
        'name': str, 
        'type':str, 
        'author':str, 
        'synthesizer': str,
        'vocal': str,
        'uploader': str
    })
    df['pubdate'] = pd.to_datetime(
        df['pubdate'],
        format='%Y-%m-%d %H:%M:%S',   # 如果格式固定，指定 format 会更快
        errors='coerce'              # 格式不对的会变成 NaT，便于后续发现与处理
    )
    df['title'] = df['title'].fillna('')      # 如果标题为空，那就空字符串
    
    return df