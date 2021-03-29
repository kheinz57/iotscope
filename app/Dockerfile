#
FROM python:3
RUN pip install influxdb paho-mqtt asyncio async-timeout bitstring pyserial-asyncio
RUN ls -l /home
#RUN python /home/mysml/setup.py

ADD smartmeter.py /home/smartmeter.py
ADD h1sml.py /home/h1sml.py
ADD h1asyncio.py /home/h1asyncio.py
#ADD localsml /home/
#ADD mysml /home/
#ADD startall.sh /home/startall.sh
#ADD smartmeter.py /home/smartmeter.py

WORKDIR /home
#CMD [ "/bin/sh", "ls -l" ]
CMD [ "python", "-u", "/home/smartmeter.py" ]
#CMD [ "/bin/sh", "/home/startall.sh"]
