from datetime import date, timedelta, datetime
from dateutil.relativedelta import relativedelta
from dataclasses import dataclass
import os

@dataclass
class BoardIdentity:
    board: str
    part: str
    issue: int
    
@dataclass
class DataIdentity:
    date: datetime


def generate_board_file_path(
    board: str,
    part: str,
    issue: int
) -> str:
    """
    输入一个榜单，返回文件的相对路径
    """
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
        return os.path.join("data", board_folder, part_folder,f"{'新曲榜' if part == 'new' else ''}{end_date.strftime('%Y%m%d')}与{start_date.strftime('%Y%m%d')}.xlsx")
    elif board == 'vocaloid-weekly':
        end_date = date(2024,8,31) + timedelta(issue*7)
        return os.path.join("data", board_folder, part_folder, f"{'新曲' if part == 'new' else ''}{end_date.strftime('%Y-%m-%d')}.xlsx")
    elif board == 'vocaloid-monthly':
        month = date(2024,6,1) + relativedelta(months=issue)
        return os.path.join("data", board_folder, part_folder, f"{'新曲' if part == 'new' else ''}{month.strftime('%Y-%m')}.xlsx")
    else:
        raise Exception('输入不符合条件')

def generate_data_file_path(
    date: datetime
) -> str:
    """
    输入一个日期，返回文件的相对路径
    """
    return os.path.join("data", "数据", date.strftime('%Y%m%d') + ".xlsx")  

def extract_file_name(filename: str) -> BoardIdentity | DataIdentity:
    """
    输入一个文件名，解析它是什么数据或者榜单文件
    """

    hyphen_count = filename.count('-')
    if hyphen_count == 1:
        result = BoardIdentity('vocaloid-monthly', 'main', 0)
        if (filename.startswith('新曲')):
            result.part = 'new'
            date_str = filename[2:]
        else:
            date_str = filename
        issue_date = datetime.strptime(date_str, '%Y-%m')
        result.issue = (issue_date.year - 2024) * 12 + issue_date.month - 6
    elif hyphen_count > 1:
        result = BoardIdentity('vocaloid-weekly', 'main', 0)
        if (filename.startswith('新曲')):
            result.part = 'new'
            date_str = filename[2:]
        else:
            date_str = filename
        issue_date = datetime.strptime(date_str, '%Y-%m-%d')
        result.issue = (issue_date - datetime(2024,8,31)).days // 7
    elif filename.count('与'):
        result = BoardIdentity('vocaloid-daily', 'main', 0)
        if (filename.startswith('新曲榜')):
            result.part = 'new'
            date_str = filename[3:]
        else:
            date_str = filename
        date_str = date_str.split('与')[0]
        issue_date = datetime.strptime(date_str, '%Y%m%d')
        result.issue = (issue_date - datetime(2024,7,3)).days
    else:
        result = DataIdentity(datetime.strptime(filename, '%Y%m%d'))
    
    return result
    