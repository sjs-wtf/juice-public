import datetime as dt

print(dt.datetime.now())

def dt14():
    seq_type = type(str(dt.datetime.now()))
    return type(str(dt.datetime.now()))().join(filter(type(str(dt.datetime.now())).isdigit, str(dt.datetime.now())))[:14]

print(dt14())