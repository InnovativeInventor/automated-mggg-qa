from pydantic import BaseModel
from typing import Optional, List, Dict


class StateRepo(BaseModel):
    state: str
    repo_name: str
    repo_account: str
    repo_url: str


class Metadata(BaseModel):
    stateLegalName: str
    stateFIPSCode: int

    git: str
    repoName: str
    archive: str
    fileName: str

    yearEffectiveStart: int
    yearEffectiveEnd: int


# JSON schema parsing
# TODO: Add comments, etc to make this easier to understand
class ElectionYearResult(BaseModel):
    USHouseAbsentee: str
    USHouseNoAbsentee: str

    USSenateAbsentee: str
    USSenateNoAbsentee: str

    USPresidentAbsentee: Optional[str]
    USPresidentNoAbsentee: Optional[str]


class PartyDescriptor(BaseModel):
    partyDescriptorFEC: str
    partyLegalName: str

    years: Dict[int, ElectionYearResult]


class ElectionPartiesDescriptor(BaseModel):
    parties: List[PartyDescriptor]


class Descriptor(BaseModel):
    stateFIPS: Optional[str]
    countyFIPS: Optional[str]
    countyLegalName: Optional[str]

    totalPopulation: Optional[str]


class StateDatafile(BaseModel):
    metadata: Metadata
    descriptors: Descriptor
    elections: ElectionPartiesDescriptor
