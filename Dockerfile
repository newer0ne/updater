FROM python:3.11.4

# Директорию внутри контейнера, где будет размещено приложение
WORKDIR /app

# Копирование файлов в контейнер
COPY updater.py /app/updater.py
COPY requirements.txt /app/requirements.txt
COPY credentials_gs.json /app/credentials_gs.json
COPY credentials_db.json /app/credentials_db.json
COPY crontab /etc/cron.d/crontab

# Установка cron
RUN apt-get update && apt-get -y install cron

# Зависимости приложения
RUN pip install -r requirements.txt

# Запускать ваше приложение внутри контейнера с помощью cron раз в день
CMD ["cron", "-f"]