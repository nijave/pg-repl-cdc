FROM python:3.12-slim

WORKDIR /app
RUN python3 -m pip install -U poetry pip setuptools wheel \
    && apt update \
    && apt install -y libpq-dev gcc


COPY python-cdc-capture/pyproject.toml python-cdc-capture/poetry.lock /app/
RUN poetry install
COPY python-cdc-capture/ /app
RUN poetry install

ENTRYPOINT ["poetry", "run", "capture"]