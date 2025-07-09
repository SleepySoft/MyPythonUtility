import datetime
import pandas as pd

from JsonSerializer import *

# ----------------------------------------------------------------------------------------------------------------------

# Import custom class here

# Note that if you import * from common, the datetime importing will be conflict

# ----------------------------------------------------------------------------------------------------------------------


register_persist_class(pd.DataFrame,        lambda py_obj: py_obj.to_dict(orient='records'),
                                            lambda json_obj: pd.DataFrame(json_obj))
register_persist_class(datetime.date,       lambda py_obj: py_obj.strftime(DATE_SERIALIZE_FORMAT),
                                            lambda json_obj: datetime.datetime.strptime(json_obj, DATE_SERIALIZE_FORMAT).date())
register_persist_class(datetime.datetime,   lambda py_obj: py_obj.strftime(DATE_TIME_SERIALIZE_FORMAT),
                                            lambda json_obj: datetime.datetime.strptime(json_obj, DATE_TIME_SERIALIZE_FORMAT))
register_persist_class(pd.Timestamp,        lambda py_obj: py_obj.strftime(DATE_TIME_SERIALIZE_FORMAT),
                                            lambda json_obj: datetime.datetime.strptime(json_obj, DATE_TIME_SERIALIZE_FORMAT))


class ExampleObject:
    pass


@JsonSerializer(ExampleObject)
def serialize_example_obj(py_obj):
    return dict(py_obj)
