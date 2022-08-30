FROM python:3.10.6-slim-buster
RUN apt update
RUN apt install -y python3-html5lib
RUN pip install poetry
COPY . /crawler
WORKDIR /crawler
RUN poetry install
