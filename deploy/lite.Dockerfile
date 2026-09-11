ARG PYTHON_IMAGE
FROM ${PYTHON_IMAGE}
WORKDIR /app
COPY pyproject.toml uv.lock ./
COPY src ./src
RUN pip install --no-cache-dir uv==0.11.28 && uv sync --frozen --no-dev
RUN useradd --uid 10001 --create-home snow && mkdir /state && chown snow:snow /state
USER 10001:10001
ENV SNOW_DB=/state/statistics.db
EXPOSE 8100
CMD ["/app/.venv/bin/snow-stats", "serve", "--host", "0.0.0.0"]
