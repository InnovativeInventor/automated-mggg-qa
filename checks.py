from description import StateSchema
import gdutils.extract
import math
from typing import Dict
from census import CensusWrapper
from utils.logger import Logger
import requests
import os
import pandas as pd

class BaseCheck():
    def __init__(self, schema: StateSchema, shapefile: gdutils.extract.ExtractTable, scratch_dir):
        self.errors = 0
        self.schema = schema
        self.shapefile = shapefile
        self.shapefile_gdf = shapefile.extract()

        self.metadata = schema.metadata
        self.descriptors = schema.descriptors

        # Setup census wrapper
        self.decentennial = math.floor(self.metadata.yearEffectiveEnd / 10) * 10
        self.census = CensusWrapper(self.decentennial, self.metadata.stateFIPSCode)

        self.scratch_dir = scratch_dir

    def file_fetch(self, url: str, filename: str):
        """
        Fetch/download file, if it does not already exist.
        """
        file_path = self.scratch_dir+filename
        print(file_path) # debug

        if not os.path.isfile(f"{file_path}"):
            data = requests.get(url)

            with open(file_path, "wb") as f:
                f.write(data.content)

        return file_path

    def aggregate_attrs_by_county(self, rows = None, fips = None) -> Dict[int, Dict[str, float]]:
        """
        Aggregate attributes by county
        TODO: Change the way defaults are set
        """
        if not rows:
            rows = map(lambda x: x[1], self.shapefile_gdf.iterrows())

        if not fips:
            fips = self.descriptors.countyFIPS

        self.county_aggregate: Dict[int, Dict[str, float]] = {}
        for each_row in rows:
            county = dict(each_row)
            each_county_fips = county[fips]
            if each_county_fips in self.county_aggregate:
                self.county_aggregate[each_county_fips] = {
                    k: (v + county[k] if isinstance(v, float) else v if v else county[k])
                    for k, v in self.county_aggregate[each_county_fips].items()
                }
            else:
                self.county_aggregate[each_county_fips] = county

        return self.county_aggregate

class TotalPopulationCheck(BaseCheck):
    def audit(self):
        # Total population check
        census_total_population = int(self.census.get_population())
        mggg_total_population = int(
            sum(self.shapefile.list_values(self.descriptors.totalPopulation))
        )

        Logger.log_info(
            f"Comparing the {self.decentennial} Census total population count ({census_total_population}) to the mggg-states count ({mggg_total_population}) in {self.metadata.repoName} for {self.metadata.yearEffectiveEnd} "
        )
        try:
            assert abs(mggg_total_population - census_total_population) <= 1
        except AssertionError as e:
            self.errors += 1
            Logger.log_error(
                f"The total population counts are off by more than 1 (off by {abs(census_total_population-mggg_total_population)})!"
            )

        return self.errors

class CountyTotalPopulationCheck(BaseCheck):
    def audit(self):
        if self.descriptors.countyFIPS:
            Logger.log_info(
                f"Checking the mggg-states county-level population count in {self.metadata.repoName} for {self.metadata.yearEffectiveEnd}"
            )
            county_aggregate = self.aggregate_attrs_by_county()

            census_county_populations = self.census.get_population(
                counties=[str(x).zfill(3) for x in county_aggregate.keys()]
            )
            for each_county_fips, each_county in county_aggregate.items():
                if self.descriptors.countyLegalName in each_county: # for logging
                    county_legal_name = each_county[self.descriptors.countyLegalName]
                else:
                    county_legal_name = "Unspecified"

                try:
                    assert each_county[self.descriptors.totalPopulation] != 0
                except AssertionError as e:
                    self.errors += 1
                    Logger.log_error(
                        f"The total population in {county_legal_name}, {self.metadata.stateAbbreviation} (FIPS={county_fips}) is zero!"
                    )

                county_fips = str(each_county_fips).zfill(3)

                try:
                    assert (
                        abs(
                            each_county[self.descriptors.totalPopulation]
                            - census_county_populations[county_fips]
                        )
                        <= 1
                    )
                except AssertionError as e:
                    self.errors += 1
                    Logger.log_error(
                        f"The total population in {county_legal_name}, {self.metadata.stateAbbreviation} (FIPS={county_fips}) differ from the US Census ({each_county[self.descriptors.totalPopulation]}!={census_county_populations[county_fips]})!"
                    )

        return self.errors

class DataExistenceCheck(BaseCheck):
    """
    Checks if required fields are always filled in the shapefile
    """
    def audit(self):
        fips_code = self.descriptors.countyFIPS

        try:
            assert all(self.shapefile.list_values(column=fips_code))

        except AssertionError as e:
            self.errors += 1
            Logger.log_warning(
                f"Not all values in {value} column in {self.metadata.repoName} for {self.metadata.yearEffectiveEnd} are filled!"
            )


class MEDSL2016Check(BaseCheck):
    def audit(self):
        county_aggregate = self.aggregate_attrs_by_county()
        file_path = self.file_fetch("https://dataverse.harvard.edu/api/access/datafile/:persistentId?persistentId=doi:10.7910/DVN/GSZG1O/BJOACP", "2016-medsl-precinct-state.csv")

        medsl_df = pd.DataFrame(file_path)
        raise NotImplemented
