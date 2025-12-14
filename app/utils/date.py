from datetime import date, timedelta
from dateutil.relativedelta import relativedelta

def get_last_census_date(today: date = date.today()) -> date:
    last_weekly_census_date = today - timedelta(days=(today.weekday()-5)%7)
    last_monthly_census_date = today.replace(day=1)
    return max(last_weekly_census_date, last_monthly_census_date)

def get_seperate_start_end_issues(board: str, issue: int) -> tuple[int, int]:
    if board == 'vocaloid-weekly':
        return issue*7+53, issue*7+59
    else:
        issue_date = date(2024, 7, 1) + relativedelta(months=issue)
        print(issue_date)
        end = (issue_date - date(2024, 8, 31)).days // 7
        print(end)
        return end-4, end