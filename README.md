# run
## prerequisites
* `.env` file with
  * INFLUX_URL
  * INFLUX_ORG
  * INFLUX_BUCKET
  * INFLUX_TOKEN
* `.netatmo.credentials` file in home directory

## local
```bash
pipenv install
pipenv run python src/main.py
```

## docker
```bash
pipenv install
pipenv requirements > requirements.txt
cp ~/.netatmo.credentials .
docker build -t netatmo2influx .
docker run --rm --env-file .env netatmo2influx 
```