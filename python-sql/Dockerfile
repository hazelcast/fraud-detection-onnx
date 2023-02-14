FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt ./requirements.txt
COPY app.py ./app.py

RUN pip3 install --upgrade pip
RUN pip3 install -r requirements.txt

EXPOSE 8501

HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health

ENTRYPOINT ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0", "--server.headless", "true", "--server.fileWatcherType", "none", "--browser.gatherUsageStats", "false"]