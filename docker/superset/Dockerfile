FROM apachesuperset.docker.scarf.sh/apache/superset:6.0.0

USER root

RUN uv pip install --python /app/.venv/bin/python trino

USER superset