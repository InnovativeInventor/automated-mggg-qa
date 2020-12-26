import tempfile
from typing import List
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
from utils.logger import Logger


class StateRepo(BaseModel):
    state: str
    repo_name: str
    repo_account: str
    repo_url: str


class CensusWrapper:
    def __init__(self, year: int, state: int):
        self.year = year
        self.state = state
        self.url = f"https://api.census.gov/data/{year}/dec/sf1?get={{fields}}&for=state:{state}"

    def get_population(self):
        result = self.fetch("P009001")
        print(result)
        return result["P009001"]

    def fetch(self, *fields):
        return dict(
            zip(
                *json.loads(
                    requests.get(self.url.format(fields=",".join(fields))).content
                )
            )
        )


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
        self.openelections_repos: Dict[StateRepo] = {
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
        self.mggg_state_repos: Dict[StateRepo] = {
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
            silent=True
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
            Logger.log_info(
                f'Auditing {each_description["metadata"]["stateLegalName"]} from {each_description["metadata"]["repoName"]}'
            )
            metadata = each_description["metadata"]
            repo_name = metadata["repoName"]
            archive_name = metadata["archive"]
            file_name = metadata["file"]
            year = metadata["yearEffectiveEnd"]
            state_fips_code = int(metadata["stateFIPSCode"])

            # Construct paths
            archive_path = self.mggg_states_dir + "/" + repo_name + "/" + archive_name
            file_path = self.expand_zipfile(archive_path) + file_name

            # Import and read shapefiles
            shapefile = gdutils.extract.read_file(file_path)
            shapefile_gdf = shapefile.extract()

            # Setup census wrapper
            decentennial = math.floor(year / 10) * 10
            census = CensusWrapper(decentennial, state_fips_code)

            # Total population check
            census_total_population = int(census.get_population())
            mggg_total_population = int(
                sum(
                    shapefile.list_values(
                        each_description["descriptors"]["totalPopulation"]
                    )
                )
            )

            Logger.log_info(
                f"Comparing the {decentennial} Census total population count ({census_total_population}) to the mggg-states count ({mggg_total_population}) in {repo_name} for {year} "
            )
            assert (
                abs(mggg_total_population - census_total_population) <= 1
            ), f"The total population counts are off by more than 1 (off by {abs(census_total_population-mggg_total_population)})!"

            print(shapefile.list_columns())


if __name__ == "__main__":
    load_dotenv()
    audit = Auditor(census_api_key=os.getenv("CENSUS_API_KEY"))
    audit.run_audit()
