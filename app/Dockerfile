#
FROM python:3
RUN pip install influxdb paho-mqtt

ADD main.py /home/main.py
ADD startall.sh /home/startall.sh
ADD calcbuffer.py /home/calcbuffer.py

WORKDIR /home
#CMD [ "python", "-u", "./main.py" ]
CMD [ "/bin/sh", "/home/startall.sh"]
