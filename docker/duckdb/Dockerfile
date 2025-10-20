FROM python:3.11

# Install apt dependencies
USER root

ARG VERSION
RUN echo "Installing DuckDB version ${VERSION}"
RUN pip3 install --no-cache-dir fastapi uvicorn simplejson pytz duckdb==${VERSION}

# Setup the entrypoint
COPY server.py /server.py
ENTRYPOINT ["python", "/server.py"]

# Expose the DuckDB port
EXPOSE 5432
