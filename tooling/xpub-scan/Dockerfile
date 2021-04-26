FROM node:14-alpine

ENV PYTHONUNBUFFERED=1

RUN apk add --no-cache --virtual .persistent-deps \
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
    && pip3 install --no-cache --upgrade pip setuptools \
    && pip3 install requests argparse

# install xpub-scan (PR)
# TODO: replace with main branch once this PR is merged
RUN cd / \
    && git clone https://github.com/LedgerHQ/xpub-scan.git \
    && cd xpub-scan \
    && git pull origin pull/29/head

WORKDIR /xpub-scan

RUN npm install
RUN tsc -p .

COPY . .

ENTRYPOINT ["python3", "check_btc.py"]