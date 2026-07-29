FROM python:3.11.11-slim-bookworm

WORKDIR /opt/ccr
COPY requirements/notebook.txt /tmp/notebook-requirements.txt
RUN pip install --no-cache-dir --disable-pip-version-check -r /tmp/notebook-requirements.txt
