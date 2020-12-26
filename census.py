import pandas as pd
import json
import requests
from typing import Union, Dict

class CensusWrapper:
    def __init__(self, year: int, state: int):
        self.year = year
        self.state = state
        self.url = f"https://api.census.gov/data/{year}/dec/sf1?get={{fields}}&for=county:{{counties}}&in=state:{state}"

    def get_population(self, counties=["*"]) -> Union[Dict[str, int], int]:
        result = self.fetch("P009001", counties=counties)
        if counties == ["*"]:
            return result["P009001"].sum()

        return dict(result["P009001"])

    def fetch(self, *fields, counties=["*"]):
        resource: str = self.url.format(
            counties=",".join(counties), fields=",".join(fields)
        )
        response = json.loads(requests.get(resource).content)

        header = response[0]
        data = [
            [int(y) if not y.startswith("0") else str(y) for y in x]
            for x in response[1:]
        ]

        return pd.DataFrame.from_records(data, columns=header, index="county")
