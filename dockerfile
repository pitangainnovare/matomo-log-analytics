FROM python:2.7-alpine AS build
COPY . /src
RUN pip install --upgrade pip \
    && pip install wheel
RUN cd /src \
    && python setup.py bdist_wheel -d /deps

FROM python:2.7-alpine
MAINTAINER scielo-dev@googlegroups.com

COPY --from=build /deps/* /deps/
COPY requirements.txt .

RUN apk add --no-cache --virtual .build-deps gcc g++ \
    && apk add --no-cache mariadb-dev libmagic \
    && pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt \
    && pip install --no-index --find-links=file:///deps -U scielo-matomo-manager \
    && apk --purge del .build-deps \
    && rm -rf /deps

WORKDIR /app

CMD ["update_available_logs", "--help"]
