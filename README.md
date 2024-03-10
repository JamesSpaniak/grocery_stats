# MA 541 Grocery Statistics

## Local Setup
Untested on windows

### MAC OS (Intel)
Setup postgresql if querying data and run python3 scripts to load data
postgresql username=postgres password=postgres

#### Running postgresql
docker compose up
brew install --cask pgadmin4
/Applications/pgAdmin\ 4.app/Contents/MacOS/pgAdmin\ 4

#### Running python3
Install python3 (3.9.6)
pip3 install -r requirements.txt
Download data file + convert to csv in data/
python3 data_processor.py

## Process
Make runner script and move permanent functions in util.py or other helper .py files for reusage