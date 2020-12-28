FROM ubuntu:18.04
ENV LANG=C.UTF-8

RUN apt-get update
RUN apt-get install -y python3 python3-pip python3-dev build-essential python3-setuptools git gcc wget
RUN pip3 install --upgrade pip

RUN python3 -m pip install -U setuptools

ADD leet_count /leet_count
ADD requirements.txt /leet_count/requirements.txt

RUN cd /leet_count && pip3 install -r requirements.txt

WORKDIR /leet_count
CMD ["python3","count_leets.py"]
