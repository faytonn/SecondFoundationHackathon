FROM python:3.11-slim

WORKDIR /app

# Copy server and galactic buffer implementation
COPY app.py .
COPY galacticbuffer.py .
COPY changePassword.py .
COPY dna.py .

EXPOSE 8080

CMD ["python", "app.py"]
