# Processing With PySpark

## How to prepare environment
```
$ conda create -n spark python=3.10
$ conda activate spark
$ pip install -r requirements.txt
```

## Example of local starting PySpark application
```
$ spark-submit ./test_task_for_sber_ai_1_1.py 2011 children
```

## How to gets results

After the application runs, file will be created in the current directory `results.json` and `restuls/` subdirectories for CSV-file
```
$ cat ./results.json | jq  # for JSON-file
$ tree -h ./results/  # for CSV-file
```
