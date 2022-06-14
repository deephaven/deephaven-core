#
# Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
#

""" Demo a as-of join on two time tables."""
import time

from pydeephaven import Session


def demo_asof_join(dh_session: Session):
    left_table = dh_session.time_table(period=100000).update(formulas=["Col1=i"])
    right_table = dh_session.time_table(period=200000).update(formulas=["Col1=i"])
    time.sleep(2)
    return left_table.aj(right_table, on=["Timestamp"], joins=["Timestamp2 = Timestamp", "Col2 = Col1"])


def main():
    with Session(host="localhost", port=10000) as dh_session:
        joined_table = demo_asof_join(dh_session)
        df = joined_table.snapshot().to_pandas()
        print(df)


if __name__ == '__main__':
    main()
