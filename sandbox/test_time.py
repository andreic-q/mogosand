from datetime import datetime, tzinfo, timezone,timedelta

def nextDatetime(start_dt):
    next_day = start_dt + timedelta(days=1)
    return next_day

start_date  = datetime(2022, 6, 16, 0, 0, 0, tzinfo=timezone.utc)
end_date    = datetime(2022, 6, 20, 0, 0, 0, tzinfo=timezone.utc) 

while (nextDatetime(start_date).date()<=datetime.now(timezone.utc).date())  and (nextDatetime(start_date).date()< end_date.date()):
      print(start_date)
      start_date =  nextDatetime(start_date)

debug_end_msg= str(datetime.now(timezone.utc))+': '+'Ended archiving in s3://'
print(debug_end_msg)
