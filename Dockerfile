FROM python:3.8

ENV ESURL "elasticsearch:9200"
ENV KIBANAURL "http://kibana:5601/api"
COPY . /
RUN pip install -r requirements.txt
CMD python3 parser.py
