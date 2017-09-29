FROM python:3.6-alpine

WORKDIR /opt
COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

COPY src .

CMD ["python", "app.py", "prod"]