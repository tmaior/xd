FROM apache/airflow:2.4.1
USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         vim \
         git \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/* 
  
RUN apt-get update \
  && apt-get -y install wget \
  && wget https://github.com/wkhtmltopdf/packaging/releases/download/0.12.6-1/wkhtmltox_0.12.6-1.buster_amd64.deb \
  && apt -y install ./wkhtmltox_0.12.6-1.buster_amd64.deb
  # add removing of the downloaded library
USER airflow
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt