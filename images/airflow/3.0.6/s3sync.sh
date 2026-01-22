echo "Synchronizing files to prod bucket s3://above-lending-prod-airflow-3-test/dags ..."
aws s3 sync ./dags "s3://above-lending-prod-airflow-3-test/dags" --delete --exact-timestamps --exclude "*.DS_Store" --exclude "*.pyc" "$@"
