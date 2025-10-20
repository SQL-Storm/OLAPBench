FROM python:3.11

ARG VERSION
RUN echo "Installing Hyper version ${VERSION}"
RUN pip3 install --no-cache-dir fastapi uvicorn simplejson tableauhyperapi==${VERSION}

# Setup the entrypoint
COPY server.py /server.py
ENTRYPOINT ["python", "/server.py"]

# Expose the Hyper port
EXPOSE 5432
