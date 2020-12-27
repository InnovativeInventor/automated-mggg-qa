from description import StateSchema
import gdutils.extract
import math
from typing import Dict
from census import CensusWrapper
from utils.logger import Logger

class BaseCheck():
    def __init__(self, schema: StateSchema, shapefile: gdutils.extract.ExtractTable):
        self.errors = 0
        self.schema = schema
        self.shapefile = shapefile
        self.shapefile_gdf = shapefile.extract()

        self.metadata = schema.metadata
        self.descriptors = schema.descriptors

        # Setup census wrapper
        self.decentennial = math.floor(self.metadata.yearEffectiveEnd / 10) * 10
        self.census = CensusWrapper(self.decentennial, self.metadata.stateFIPSCode)


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

            # Aggregate by county
            county_aggregate: Dict[int, Dict[str, float]] = {}
            for each_county_fips, each_county in self.shapefile_gdf.iterrows():
                county = dict(each_county)
                if each_county_fips in county_aggregate:
                    county_aggregate[each_county_fips] = {
                        k: (v + county[k] if isinstance(v, float) else v if v else county[k])
                        for k, v in county_aggregate[each_county_fips].items()
                    }
                else:
                    county_aggregate[each_county_fips] = county

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
    def audit(self):
        expected_values = []
        if self.descriptors.countyFIPS:
            expected_values.append(self.descriptors.countyFIPS)

        for value in expected_values:
            try:
                assert all(self.shapefile.list_values(column=value))

            except AssertionError as e:
                self.errors += 1
                Logger.log_warning(
                    f"Not all values in {value} column in {self.metadata.repoName} for {self.metadata.yearEffectiveEnd} are filled!"
                )
