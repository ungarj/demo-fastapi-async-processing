# FROM tiangolo/uvicorn-gunicorn-fastapi:python3.7
FROM registry.gitlab.eox.at/maps/docker-base/mapchete:0.20

COPY ./requirements.txt /app/

RUN pip install -r /app/requirements.txt

COPY . /app/app

WORKDIR /app/app

RUN pip install -e .