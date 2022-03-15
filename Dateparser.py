from dateutil.parser import parse
from datetime import datetime


def parsetime(text: str):
    time_text = text
    time_time = ""
    try:
        time_parse = parse(text, fuzzy_with_tokens=True)
        time_text = time_parse[1]
        time_time = time_parse[0]
        year_now = datetime.today().year
        month_now = datetime.today().month
        year_parse = time_time.year
        month_parse = time_time.month
        if (year_now == year_parse) and (month_now == month_parse):
            date_time = False
        else:
            date_time = True
        if "th century" in text:
            date_time = True
        if str(year_now) == text:
            date_time = True
        if "(number)" in text:
            date_time = False
        return time_text, time_time, date_time
    except Exception as ex:
        return time_text, time_time, False


if __name__ == "__main__":
    print(parsetime("May 1970"))