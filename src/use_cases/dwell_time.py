import pandas as pd

def dwell_split(df, split_intervals):
    stay_time = df.stay_time.values
    
    split_labels = [f"<= {int(i/60)}" for i in split_intervals[1:]]
    if len(stay_time) == 0:
        time_counts = [0 for i in range(len(split_labels))]
    else:
        ii = pd.IntervalIndex.from_tuples([(split_intervals[i], split_intervals[i+1]) for i in range(len(split_intervals)-1)],closed="right")
        time_counts = df.groupby(pd.cut(stay_time,ii)).size().values
    
    return pd.DataFrame.from_dict({"dwell_time_interval": split_labels,
                                   "count_people": time_counts})


def dwell_split_empty(day, polygon_id, split_intervals, hourly):
    
    split_labels = [f"<= {int(i/60)}" for i in split_intervals[1:]]
    residency_labels = ["total", "resident", "national tourist", 'international tourist', 
                       "regular visitor", "casual visitor", "commuter"]
    if hourly:
    
        datetimes = [pd.to_datetime(f"{day} {hour:02d}:00:00", utc=True) for hour in range(24)] 
        slabs = [sl for sl in split_labels for _ in datetimes for _ in residency_labels]
        ds = [d for _ in split_labels for d in datetimes for _ in residency_labels]
        rs = [r for _ in split_labels for _ in datetimes for r in residency_labels]
        ps = [polygon_id for _ in rs]
        c = [0 for _ in rs]
        out_d = dict(polygon_id = ps, datetime = ds, residency_status = rs, dwell_time_interval = slabs, count_people = c)
        
    else:
        
        date = [f"{day.date()}"]
        slabs = [sl for sl in split_labels for _ in date for _ in residency_labels]
        ds = [d for _ in split_labels for d in date for _ in residency_labels]
        rs = [r for _ in split_labels for _ in date for r in residency_labels]
        ps = [polygon_id for _ in rs]
        c = [0 for _ in rs]
        out_d = dict(polygon_id = ps, date = ds, residency_status = rs, dwell_time_interval = slabs, count_people = c)
        
    return pd.DataFrame.from_dict(out_d)