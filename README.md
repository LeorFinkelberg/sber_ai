# Processing With PySpark

## How to prepare environment
```
$ git clone https://github.com/LeorFinkelberg/sber_ai.git
$ cd sber_ai/
$ export ENV_NAME=spark
$ conda create -n ${ENV_NAME} python=3.10
$ conda activate ${ENV_NAME}
$ pip install -r requirements.txt
$ curl -O https://files.grouplens.org/datasets/movielens/ml-25m.zip
$ unzip ml-25m.zip
$ pre-commit install
```

## Example of local starting PySpark application
```
$ spark-submit ./run.py 2011 children
```

## Where to look for the results of the application

After the application runs, file `results.json` will be created in the current directory
and `restuls/` subdirectories for CSV-file
```
$ cat ./results.json | jq  # for JSON-file
{
  "Back to the Future Part II (1989)": [
    436,
    1643,
    4931,
    6126,
    2942
  ],
  "hist_all": [
    776815,
    1640868,
    4896928,
    6639798,
    3612474
  ]
}

$ tree -h ./results/  # for CSV-file
- part-*.csv
- _SUCCESS
```
