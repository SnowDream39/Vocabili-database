
def make_duration_str(duration: int | None):
    """
    将时长转换为字符串
    """
    if duration is None:
        return None
    if duration < 60:
        return f"{duration}秒"
    else:
        return f"{duration // 60}分{duration % 60}秒"

def make_duration_int(duration: str) -> int | None:
    """
    将时长字符串转换为整数
    """
    if duration is None:
        return None
    if '分' not in duration:
        return int(duration.split('秒')[0])
    else:
        return int(duration.split('分')[0]) * 60 + int(duration.split('分')[1].split('秒')[0])

def make_artist_str(artists) -> str | None:
    """
    将艺术家列表转换为字符串
    """
    if artists is None:
        return None
    return '、'.join(map(lambda x: x.name, artists))
