# Build container conda env
FROM python:3.7

ENV LIBRARY_VERSION 0.0.7
RUN pip install "fh-immuta-utils==${LIBRARY_VERSION}"

ENTRYPOINT "fh-immuta-utils"

