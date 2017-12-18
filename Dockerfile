FROM python:3.6-alpine

WORKDIR /opt
COPY requirements.txt requirements.txt
# find a way around this
RUN apk update
RUN apk upgrade
RUN apk add git
RUN pip3 install -r requirements.txt

COPY src .

CMD ["python", "app.py", "prod"]