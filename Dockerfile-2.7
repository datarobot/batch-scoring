FROM python:2.7

COPY requirements.txt /opt/project/requirements.txt

WORKDIR /opt/project

RUN grep -v '^-e .' requirements.txt >/tmp/requirements.txt && \
	pip install -r /tmp/requirements.txt && \
	rm /tmp/requirements.txt

COPY . /opt/project

RUN pip install -e .

COPY docker-entrypoint.sh /

RUN chmod +x /docker-entrypoint.sh

ENTRYPOINT ["/docker-entrypoint.sh"]

CMD ["/bin/bash"]
