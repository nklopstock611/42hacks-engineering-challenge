# 42hacks-engineering-challenge

https://github.com/Un-Real-AI/data_engineering_challenge

## How to run:

1. Clone the repository
2. Run `pip install -r requirements.txt`
3. Run Data Load `python -m database.db` (**NOT RECOMMENDED** - data is already loaded)
4. Run API `python -m uvicorn app.airports_api:app`

Deployed on: https://four2hacks-engineering-challenge.onrender.com

Because it is a free service, the first request may have a 50s re-activation delay and an error response...

It is recommended to run the API locally and test on localhost.
