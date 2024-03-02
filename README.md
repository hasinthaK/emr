# UoM MapReduce vs Spark Assignment Scripts

## Preparing S3 for scripts

PySpark & Hive Scripts use a set of predetermined S3 locations hence the directories can be created upfront if needed. This process is automated via a simple python script. Follow below instructions to create the directory structure on your AWS S3 bucket.

1. Create a Python virtual environment using `python -m venv venv`.
2. Activate the virtual environment by appropriate script for your OS from inside `venv/Scripts`.
3. Install needed packages in `requirements.txt` using `pip install -r requirements.txt`.
4. Make sure to set your AWS access keys in either environment variables or config file - Refer Boto3 [docs](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/configuration.html#using-environment-variables).
5. Run `s3-directory-strcuture-init.py`.

## Running Hive Script

- Make sure your Hadoop EMR cluster is started & in waiting state.
- Add a step with the type Hive Application with the script & specify the S3 locations.

Hive script can be run multiple times with a CLI argument `ITERATION`. Specify the current index of iteration strating from 0. In EMR Step, add `-hivevar ITERATION=<iteration index>`. ie: `hivevar ITERATION=0` for first iteration.
In each iteration, the source csv might get removed from S3, if so please upload it again & run the next iteration as a new step with the new `ITERATION`.

## Runnin PySpark Script

- Make sure your Spark EMR cluster is started & in waiting state.
- Add a step with the type Spark Application with the script & specify the S3 locations. 
No CLI arguments are supported as script itself loops for 5 times.

</br>

---

To download the S3 results to your local machine, you may use AWS CLI.
ie: `aws s3 cp s3://<s3-folder> <destination-folder> --recursive --profile learner-lab`
Before running above ensure that AWS CLI ACCESS_KEY & SECRET_ACCESS_KEY are in place. (`~/.aws/credentials`). Also create a new aws profile named `learner-lab` with aws cli credentials.
