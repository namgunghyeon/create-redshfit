FROM ubuntu:18.04

RUN apt-get update && apt-get upgrade -y
RUN apt-get install -y python3 python3-pip curl
RUN pip3 install awscli

WORKDIR /project
ADD ./ /project

RUN pip3 install -r requirements.txt
CMD python3 create-redshift.py