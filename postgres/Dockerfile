FROM postgres:15-alpine

# Install build dependencies and extensions
RUN apk add --no-cache --virtual .build-deps \
    build-base \
    postgresql-dev \
    git \
    cmake \
    && apk add --no-cache \
    postgis \
    postgresql-contrib \
    # Other dependencies that might be needed
    libstdc++ \
    && mkdir -p /usr/local/src

# Install pgvector extension
RUN cd /usr/local/src \
    && git clone --branch v0.4.1 https://github.com/pgvector/pgvector.git \
    && cd pgvector \
    && make \
    && make install \
    # Clean up
    && apk del .build-deps \
    && rm -rf /usr/local/src/pgvector

# Add initialization scripts
COPY ./init-extensions.sql /docker-entrypoint-initdb.d/10-init-extensions.sql