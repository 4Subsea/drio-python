import io

import pandas as pd


class DataHandler:
    """
    Handles conversion of data series.

    Parameters
    ----------
    series : pandas.Series
        Data as series.
    """

    def __init__(self, series):
        if not isinstance(series, pd.Series):
            raise ValueError()
        if not series.name == "values":
            raise ValueError()
        if series.index.name is not None:
            raise ValueError()

        self._series = series

    @classmethod
    def from_csv(cls, path):
        """Read data from CSV file"""
        df = pd.read_csv(
            path,
            header=None,
            names=("index", "values"),
            dtype={"index": "int64", "values": "str"},
            encoding="utf-8",
        ).astype({"values": "float64"}, errors="ignore")

        series = df.set_index("index").squeeze("columns")
        series.index.name = None

        return cls(series)

    def as_series(self):
        """Return data as a ``pandas.Series`` object."""
        return self._series.copy(deep=True)

    def as_dataframe(self):
        """Return data as a ``pandas.DataFrame`` object."""
        return self.as_series().reset_index()

    def as_binary_csv(self):
        """Return data as a binary string (representing tha data in CSV format)."""
        df = self.as_dataframe()
        with io.BytesIO() as fp:
            kwargs = {
                "header": False,
                "index": False,
                "encoding": "utf-8",
                "mode": "wb",
            }
            df.to_csv(fp, lineterminator="\n", **kwargs)
            csv = fp.getvalue()
        return csv