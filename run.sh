if [ ! -d ".venv" ]; then
    python ./create_venvs.py --target development
fi

source .venv/bin/activate \
    && cd ./images/airflow/3.0.6/ \
    && ./run.sh
