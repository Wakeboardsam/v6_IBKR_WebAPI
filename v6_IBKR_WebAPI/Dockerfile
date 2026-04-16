FROM python:3.11-alpine

# Install bash and jq so we can parse Home Assistant's option variables
RUN apk add --no-cache bash jq

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt || true

COPY . .
RUN chmod a+x run.sh

ENTRYPOINT [ "/app/run.sh" ]
