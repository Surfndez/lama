FROM node:14-alpine

ENV PYTHONUNBUFFERED=1

RUN apk add --quiet --no-cache --virtual .persistent-deps \
        curl \
        openssl \
        make \
        git \
        gcc \
        g++ \
        python3 && ln -sf python3 /usr/bin/python \
        py-pip \
    && npm install --silent --save-dev -g \
        typescript

RUN python3 -m ensurepip \
    && pip3 -q install --no-cache --upgrade pip setuptools \
    && pip3 -q install requests argparse

RUN cd / && git clone --quiet https://github.com/LedgerHQ/xpub-scan.git

WORKDIR /xpub-scan

RUN mkdir -p /xpub-scan/report

RUN ( npm install &> /dev/null ) && tsc -p .

COPY . .

ENTRYPOINT ["python3", "check_btc.py"]