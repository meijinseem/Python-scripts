# -------------  Dockerfile --------------
FROM python:latest 

RUN apt-get update && \
    apt-get install -y unixodbc-dev && \
    apt-get install -y curl && \
    curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && \
    curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update && \
    ACCEPT_EULA=Y apt-get install -y msodbcsql17

# Установка необходимых библиотек Python
RUN pip install pandas pyodbc requests numpy

ENV LD_LIBRARY_PATH=/usr/local/lib:/opt/microsoft/msodbcsql17/lib64:$LD_LIBRARY_PATH

COPY . /app

WORKDIR /app

CMD ["python", "app-ym-mssql.py"]
