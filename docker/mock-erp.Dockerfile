# Wraps the provided harness without changing it.
FROM python:3.12-slim
RUN pip install --no-cache-dir fastapi uvicorn
WORKDIR /app
COPY harness/ /app
EXPOSE 9100
CMD ["uvicorn", "mock_erp:app", "--host", "0.0.0.0", "--port", "9100"]
