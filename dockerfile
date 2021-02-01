FROM python:2.7-alpine AS build
COPY . /src
RUN pip install --upgrade pip \
    && pip install wheel
RUN cd /src \
    && python setup.py bdist_wheel -d /deps

FROM python:2.7-alpine
MAINTAINER scielo-dev@googlegroups.com

COPY . /app

WORKDIR /app

RUN apk add --update \
    && apk add gcc g++ mariadb-dev \
    && pip install --upgrade pip

RUN python setup.py install

CMD ["update_available_logs", "--help"]
