# Le container de depart, que je veux modifier

FROM python:3.9-slim

# définition du dossier de travail
WORKDIR /app

# copy du fichier requirements.txt vers /app
COPY requirements.txt .

# installatrion des despendances
RUN pip install --no-cache-dir -r requirements.txt

#Lancement du projet
CMD ["python", "consumer.py"]