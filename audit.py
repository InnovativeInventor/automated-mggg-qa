import tempfile
from typing import List, Dict, Union
import glob
import gdutils
import gdutils.extract
import json
import gdutils.datamine as dm
import gdutils.dataqa as dq
import re
from pydantic import BaseModel
import zipfile
import math
import requests
import os
from dotenv import load_dotenv
import pandas as pd
from utils.logger import Logger
from description import StateRepo


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


class Auditor:
    """
    Auditor class for (automated) mggg-states QA against openelections data
    """

    def __init__(self, census_api_key: str):
        # self.census_api = Census(census_api_key)

        self.openelections_dir: str = tempfile.TemporaryDirectory(
            suffix="openelections"
        ).name
        self.mggg_states_dir: str = tempfile.TemporaryDirectory(
            suffix="mggg-states"
        ).name

        # Generate and filter list of openelections repos
        state_expr = re.compile(r"^openelections-data-\S\S$")
        self.openelections_repos: Dict[str, StateRepo] = {
            repo_name: StateRepo(
                state=repo_name.split("-")[-1],
                repo_name=repo_name,
                repo_account="openelections",
                repo_url=repo_url,
            )
            for repo_name, repo_url in dm.list_gh_repos(
                account="openelections", account_type="orgs"
            )
            if state_expr.match(repo_name)
        }
        self.mggg_state_repos: Dict[str, StateRepo] = {
            repo_name: StateRepo(
                state=repo_name.split("-")[0],
                repo_name=repo_name,
                repo_account="mggg-states",
                repo_url=repo_url,
            )
            for repo_name, repo_url in dm.list_gh_repos(
                account="mggg-states", account_type="orgs"
            )
        }

        self.audit_repos = set()
        self.descriptors: List[dict] = [
            self.load_descriptor(x) for x in glob.glob("descriptions/*.json")
        ]
        for each_item in self.descriptors:
            self.audit_repos.add(each_item["metadata"]["repoName"])

        dm.clone_gh_repos(
            account="mggg-states",
            account_type="orgs",
            repos=list(self.audit_repos),
            outpath=self.mggg_states_dir,
            silent=True,
        )
        # dm.clone_gh_repos(account="openelections", account_type="orgs", repos=map(lambda x: x.repo_name, self.openelections_repos), outpath=self.openelections_dir)

    @staticmethod
    def load_descriptor(filename: str) -> dict:
        with open(filename) as f:
            return json.load(f)

    def expand_zipfile(self, archive_path: str) -> str:
        """
        Expands zipped files in directory for QA work.
        Returns the name of the directory
        """
        output_dir = archive_path.replace(".zip", "/")
        with zipfile.ZipFile(archive_path, "r") as zip_ref:
            zip_ref.extractall(output_dir)

        return output_dir

    def run_audit(self):
        """
        Runs audits on all states with descriptors
        """
        for each_description in self.descriptors:
            # Extract useful metadata from dict
            metadata = each_description["metadata"]
            repo_name = metadata["repoName"]
            archive_name = metadata["archive"]
            file_name = metadata["file"]
            year = metadata["yearEffectiveEnd"]
            state_fips_code = int(metadata["stateFIPSCode"])
            Logger.log_info(
                f'Auditing {metadata["archive"]} in {each_description["metadata"]["stateLegalName"]} from {repo_name}.'
            )

            try:
                # Construct paths
                archive_path = (
                    self.mggg_states_dir + "/" + repo_name + "/" + archive_name
                )
                file_path = self.expand_zipfile(archive_path) + file_name

                # Find column names
                total_population_col = each_description["descriptors"][
                    "totalPopulation"
                ]
                county_fips_col = each_description["descriptors"]["countyFIPS"]
                county_legal_name = each_description["descriptors"]["countyLegalName"]

                # Import and read shapefiles
                if county_fips_col:
                    shapefile = gdutils.extract.read_file(
                        file_path, column=county_fips_col
                    )
                else:
                    shapefile = gdutils.extract.read_file(file_path)

                shapefile_gdf = shapefile.extract()

                # Setup census wrapper
                decentennial = math.floor(year / 10) * 10
                census = CensusWrapper(decentennial, state_fips_code)

                # Total population check
                census_total_population = int(census.get_population())
                mggg_total_population = int(
                    sum(shapefile.list_values(total_population_col))
                )

                Logger.log_info(
                    f"Comparing the {decentennial} Census total population count ({census_total_population}) to the mggg-states count ({mggg_total_population}) in {repo_name} for {year} "
                )
                try:
                    assert abs(mggg_total_population - census_total_population) <= 1
                except AssertionError as e:
                    Logger.log_error(
                        f"The total population counts are off by more than 1 (off by {abs(census_total_population-mggg_total_population)})!"
                    )

                # County level checks
                if county_fips_col:
                    Logger.log_info(
                        f"Checking the mggg-states county-level population count ({mggg_total_population}) in {repo_name} for {year} "
                    )

                    # Aggregate by county
                    county_aggregate: Dict[int, Dict[str, float]] = {}
                    for each_county_fips, each_county in shapefile_gdf.iterrows():
                        county = dict(each_county)
                        if each_county_fips in county_aggregate:
                            county_aggregate[each_county_fips] = {
                                k: (v + county[k] if isinstance(v, float) else v)
                                for k, v in county_aggregate[each_county_fips].items()
                            }
                        else:
                            county_aggregate[each_county_fips] = county

                    census_county_populations = census.get_population(
                        counties=[str(x).zfill(3) for x in county_aggregate.keys()]
                    )
                    for each_county_fips, each_county in county_aggregate.items():
                        try:
                            assert each_county[total_population_col] != 0
                        except AssertionError as e:
                            Logger.log_error(
                                f"The total population in {each_county[county_legal_name]} county (FIPS {each_county_fips}) is zero!"
                            )

                        county_fips = str(each_county_fips).zfill(3)
                        try:
                            assert (
                                abs(
                                    each_county[total_population_col]
                                    - census_county_populations[county_fips]
                                )
                                <= 1
                            )
                        except AssertionError as e:
                            Logger.log_error(
                                f"The mggg-states total population in {each_county[county_legal_name]} county (FIPS {each_county_fips}) are not close to the US Census ({each_county[total_population_col]}!={census_county_population})!"
                            )

            except KeyboardInterrupt:
                Logger.log_info(
                    f'Captured KeyboardInterrupt! Skipping {metadata["archive"]} in {each_description["metadata"]["stateLegalName"]} from {each_description["metadata"]["repoName"]}!'
                )
                pass


if __name__ == "__main__":
    load_dotenv()

    if census_api_key := os.getenv("CENSUS_API_KEY"):
        audit = Auditor(census_api_key=census_api_key)
        audit.run_audit()

    else:
        Logger.log_warning("Cannot find Census API key!")
