import tempfile
import checks
from census import CensusWrapper
from typing import List, Dict, Union
import glob
import gdutils
import gdutils.extract
import json
import gdutils.datamine as dm
import gdutils.dataqa as dq
import re
import zipfile
import math
# import os
# from dotenv import load_dotenv
from utils.logger import Logger
from description import StateRepo, StateSchema


class Auditor:
    """
    Auditor class for (automated) mggg-states QA against openelections data
    """

    def __init__(self):
    # def __init__(self, census_api_key: str):
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
        self.descriptor_files: List[dict] = [
            self.load_descriptor(x) for x in glob.glob("descriptions/*.json")
        ]
        for each_item in self.descriptor_files:
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
        for each_description in self.descriptor_files:
            schema = StateSchema(**each_description)

            metadata = schema.metadata
            descriptors = schema.descriptors

            Logger.log_info(
                f'Auditing {metadata.archive} in {metadata.stateLegalName} from {metadata.repoName}.'
            )

            try:
                # Construct paths
                archive_path = (
                    self.mggg_states_dir + "/" + metadata.repoName + "/" + metadata.archive
                )
                file_path = self.expand_zipfile(archive_path) + metadata.fileName

                # Find column names
                total_population_col = descriptors.totalPopulation
                county_fips_col = descriptors.countyFIPS
                county_legal_name = descriptors.countyLegalName

                # Import and read shapefiles
                if county_fips_col:
                    shapefile = gdutils.extract.read_file(
                        file_path, column=county_fips_col
                    )
                else:
                    shapefile = gdutils.extract.read_file(file_path)


                total_population_check = checks.TotalPopulationCheck(schema, shapefile)
                total_population_check.audit()

                county_population_check = checks.CountyTotalPopulationCheck(schema, shapefile)
                county_population_check.audit()

            except KeyboardInterrupt:
                Logger.log_info(
                    f'Captured KeyboardInterrupt! Skipping {metadata["archive"]} in {each_description["metadata"]["stateLegalName"]} from {each_description["metadata"]["repoName"]}!'
                )
                pass


if __name__ == "__main__":
    # load_dotenv()

    # if census_api_key := os.getenv("CENSUS_API_KEY"):
    #     audit = Auditor(census_api_key=census_api_key)
    #     audit.run_audit()

    # else:
    #     Logger.log_warning("Cannot find Census API key!")

    audit = Auditor()
    audit.run_audit()
