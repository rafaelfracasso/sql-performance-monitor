FROM python:3.12-slim

WORKDIR /app

# Dependencias de sistema + Microsoft ODBC Driver 18 for SQL Server
RUN apt-get update && apt-get install -y --no-install-recommends \
        curl \
        gnupg \
        unixodbc-dev \
    && curl -fsSL https://packages.microsoft.com/keys/microsoft.asc \
        | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg \
    && curl -fsSL https://packages.microsoft.com/config/debian/12/prod.list \
        -o /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y --no-install-recommends msodbcsql18 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Dependencias Python (camada de cache separada)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Codigo-fonte
COPY . .

# Diretorio de dados persistidos (DuckDB + logs)
RUN mkdir -p logs

EXPOSE 8080

CMD ["python", "main.py"]
