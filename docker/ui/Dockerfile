FROM python:3.11.11-slim-bookworm
WORKDIR /opt/ccr
COPY requirements/ui.txt /tmp/requirements.txt
RUN pip install --no-cache-dir --disable-pip-version-check -r /tmp/requirements.txt
