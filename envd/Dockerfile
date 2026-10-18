FROM python:3.12-slim

# Non-root user the sandbox code runs as
RUN useradd -m -s /bin/bash user \
 && pip install --no-cache-dir fastapi "uvicorn[standard]"

COPY envd/server.py /opt/envd/server.py

USER user
WORKDIR /home/user
ENV WORKDIR=/home/user
EXPOSE 49983

# ENVD_TOKEN is injected by the control plane at container start
CMD ["python", "-m", "uvicorn", "--app-dir", "/opt/envd", "server:app", "--host", "0.0.0.0", "--port", "49983"]
