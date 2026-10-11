FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY pyproject.toml README.md ./
COPY etl_framework ./etl_framework
COPY tests ./tests
COPY configs ./configs
COPY ddl ./ddl
COPY examples ./examples

RUN pip install --no-cache-dir -e .

CMD ["python", "-m", "unittest", "discover", "-s", "tests"]

