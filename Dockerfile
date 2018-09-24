FROM python:3.6

RUN pip3 install --no-cache-dir -r requirements.txt


ENV PYTHONBUFFERED=TRUE
ENV PYTHONDONTWRITEBYTECODE=TRUE
ENV PATH="${PATH}"


