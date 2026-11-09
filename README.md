## ms-iris-credit-risk
This is a sample of an InterSystems IRIS microservice.
The sample allows you calculate credit risk analysis from a microservice using REST API or Kafka events.

## Prerequisites
Make sure you have [git](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git) and [Docker desktop](https://www.docker.com/products/docker-desktop) installed.

## Installation 

Clone/git pull the repo into any local directory

```
$ git clone https://github.com/yurimarx/ms-iris-credit-risk.git
```

Open the terminal in this directory and run:

```
$ docker-compose build
```

3. Run the IRIS container with your project:

```
$ docker-compose up -d
```

## How to Test it - REST API

Import the postman collection https://github.com/yurimarx/ms-iris-credit-risk/blob/master/Credit%20Risk%20API.postman_collection.json or the local file Credit Risk API.postman_collection.json


## How to Test it - Kafka API

1. Open and Start the production http://localhost:52795/csp/user/EnsPortal.ProductionConfig.zen?PRODUCTION=dc.creditrisk.CreditRiskProduction
2. Open Kafka UI: http://localhost:18080/
3. Go to Topics and create a CreditRiskInTopic if not exists.
4. Click the CreditRiskInTopic link and click Produce Message (on the top right corner of the page)
5. Fills iris into Key and the following json data into Value:
{
    "Age": 42,
    "Sex": "male",
    "Job": 1,
    "Housing": "own",
    "SavingAccounts": "rich",
    "CheckingAccount": "little",
    "CreditAmount": 10000,
    "Duration": 6,
    "Purpose": "car"
}
6. Click button Produce
7. Go to Topics and click the CreditRiskOutTopic link
8. Go to Messages and see the response message sent
9. See the request and response flow on the Interoperabitiy production also. 


## Python version (intersystems-pyprod)

`python/creditrisk_pyprod.py` re-implements the Kafka Business Service and Business Process in pure Python with [intersystems-pyprod](https://pypi.org/project/intersystems-pyprod/). It also defines a Python production, `dc.creditrisk.pyprod.CreditRiskPyProduction`.

```
pip install intersystems-pyprod
intersystems_pyprod /full/path/to/python/creditrisk_pyprod.py
```

Set the IRIS connection environment variables first (see the pyprod installing guide). Both productions use the same Kafka topics, so run only one at a time.

### References

1. Dataset used to train the model: https://www.kaggle.com/datasets/benjaminmcgregor/german-credit-data-set-with-credit-risk

