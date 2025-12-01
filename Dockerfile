FROM python:3.9-slim-buster

WORKDIR /app

COPY run-cw-sync.sh connectwise-case-sync.py ConnectWise.py STELLAR_UTIL.py LOGGER_UTIL.py requirements.txt /app/

RUN mkdir -p /app/data
RUN chmod o+rwx /app/run-cw-sync.sh

RUN pip install --no-cache-dir -r requirements.txt

CMD ["/app/run-cw-sync.sh"]
