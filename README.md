## Automated MGGG QA Checks
Each repo in `mggg-states` has a corresponding descriptor file (for now, in this repo's `/descriptions`). 
In the future each, descriptor file will be in each respective data repo.
The descriptor files are used to standardize and uniformly test each shapefile's attributes.

Work in progress.

## Running (with poetry)
```python
pip install poetry
poetry install
poetry run audit.py
```

## Contributions
Uses [MGGG's gdutils](https://github.com/mggg/gdutils) library for repo cloning and data extraction.

All contributions are welcome.

License: `MIT License`


