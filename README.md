# takp2peq_character_migration
Character Migration Tool from TAKP to PEQ Database

## Quickstart
1. Set up
```sh
git clone https://github.com/EQArchives/takp2peq_character_migration.git
cp .env.example .env
python3 -m venv venv
source venv.bin/activate
pip install -r requirements.txt
```
2. Edit configuration file
The script loads configuration files from the `.env` file:
```sh
HOST="localhost"
USERNAME="eqemu"
PASSWD=""
EQEMU_DATABASE="peq"
EQMACEMU_DATABASE="takp"
```
3. Run the script
```
(venv) $ python3 migrate.py --help
usage: migrate.py [-h] [-c CHARACTER]

TAKP to PEQ character transfer tool

options:
  -h, --help            show this help message and exit
  -c, --character CHARACTER
```
