FROM prefecthq/prefect:2.16-python3.9

WORKDIR /opt/prefect

COPY docker-requirements.txt ./
COPY ./dbt ./dbt
COPY ./credentials ./credentials
COPY ./pipeline ./pipeline
COPY .kaggle /root/.kaggle

RUN pip install -r docker-requirements.txt \
    --trusted-host pypi.python.org --no-cache-dir
