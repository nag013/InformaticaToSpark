import unittest

from etl_framework.writers import WriteTarget, write_data


class FakeWriter:
    def __init__(self):
        self.calls = []

    def format(self, value):
        self.calls.append(("format", value))
        return self

    def mode(self, value):
        self.calls.append(("mode", value))
        return self

    def option(self, key, value):
        self.calls.append(("option", key, value))
        return self

    def partitionBy(self, *columns):
        self.calls.append(("partitionBy", columns))
        return self

    def saveAsTable(self, table):
        self.calls.append(("saveAsTable", table))

    def save(self, path):
        self.calls.append(("save", path))


class FakeDataFrame:
    def __init__(self):
        self.write = FakeWriter()


class WriterTests(unittest.TestCase):
    def test_write_data_writes_table(self):
        df = FakeDataFrame()

        write_data(
            df,
            WriteTarget(
                table="silver.orders",
                fmt="delta",
                mode="overwrite",
                partition_by=("dt",),
                options={"mergeSchema": "true"},
            ),
        )

        self.assertEqual(
            df.write.calls,
            [
                ("format", "delta"),
                ("mode", "overwrite"),
                ("option", "mergeSchema", "true"),
                ("partitionBy", ("dt",)),
                ("saveAsTable", "silver.orders"),
            ],
        )

    def test_write_target_requires_one_destination(self):
        with self.assertRaisesRegex(Exception, "exactly one"):
            WriteTarget()


if __name__ == "__main__":
    unittest.main()

