# run
## prerequisites
* file `./.env` defining:
  * `INFLUX_URL`
  * `INFLUX_ORG`
  * `INFLUX_BUCKET`
  * `INFLUX_TOKEN`
  * `READ_INTERVAL`
* file `~/.netatmo.credentials`

## local
```bash
pipenv install
pipenv run python src/main.py
```

## docker
```bash
pipenv install
pipenv requirements > requirements.txt
docker build -t netatmo2influx .
docker run \
  --env-file .env \
  -v ~/.netatmo.credentials:/root/.netatmo.credentials:ro \
  --name netatmo2influx \
  netatmo2influx:latest
docker start -a netatmo2influx
```