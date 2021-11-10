FROM python:3.10-alpine

WORKDIR /code
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt
COPY . .
RUN python setup.py install

CMD ["monitoring_controller"]