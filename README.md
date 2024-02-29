# UoM MapReduce vs Spark Assignment Scripts

## Preparing S3 for scripts

PySpark & Hive Scripts use a set of predetermined S3 locations hence the directories need to be created upfront. This process is automated via a simple python script. Follow below instructions to create the directory structure on your AWS S3 bucket.

1. Create a Python virtual environment using `python -m venv venv`.
2. Activate the virtual environment by appropriate script for your OS from inside `venv/Scripts`.
3. Install needed packages in `requirements.txt` using `pip install -r requirements.txt`.
4. Make sure to set your AWS access keys in either environment variables or config file - Refer Boto3 [docs](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html#using-environment-variables).
5. Run `s3-directory-strcuture-init.py`.
