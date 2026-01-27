# Download and build the wheels for the requirements.
awk 'NR > 3' ./requirements/mwaa-requirements.txt > ./requirements/download-requirements.txt 
rm -rf ./wheels/
rm -rf ./plugins/plugins.zip
docker run --platform linux/amd64 --rm -v $(pwd):/workspace -w /workspace python:3.12-slim \
  pip download -r ./requirements/download-requirements.txt \
  --constraint https://raw.githubusercontent.com/apache/airflow/constraints-3.0.6/constraints-3.12.txt \
  -d wheels/
rm -rf ./requirements/download-requirements.txt

# Zip the wheels along with the constraints file.
zip -j plugins/plugins.zip wheels/* ./requirements/mwaa_essential_constraints.txt

# Upload the plugins.zip and requirements.txt to the S3 bucket.
aws s3 cp plugins/plugins.zip s3://above-lending-prod-airflow-3/plugins.zip

aws s3 cp ./requirements/mwaa-requirements.txt s3://above-lending-prod-airflow-3/requirements.txt