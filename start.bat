@echo off
title Lancement du Projet Streaming E-commerce
color 0A

echo [1/8] Demarrage de Zookeeper...
start "Zookeeper" cmd /k "cd C:\kafka && .\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties

timeout /t 30 /nobreak > NUL


echo [2/8] Demarrage de Kafka...
start "Kafka" cmd /k "cd C:\kafka && .\bin\windows\kafka-server-start.bat .\config\server.properties"

timeout /t 15 /nobreak > NUL

echo [3/8] Preparation de la Base de Donnees MySQL...
start "Setup Database" /WAIT cmd /c "python raw_data.py"
start "Setup Database" /WAIT cmd /c "python star_schema_init.py"

timeout /t 5 /nobreak > NUL

echo [4/8] Lancement du Producer Kafka...
start "Producer Kafka" cmd /k "python producer.py"

timeout /t 5 /nobreak > NUL

echo [5/8] Lancement du Consumer Spark...
start "Consumer Spark" cmd /k "python consumer.py"
timeout /t 10 /nobreak > NUL
echo [6/8] Lancement du Consumer Spark...
start "star_schema" cmd /k "python star_schema.py"
timeout /t 10 /nobreak > NUL
echo [7/8] Lancement du modele ALS (Recommandations)...
start "ALS Model" cmd /k "python als.py"
timeout /t 10 /nobreak > NUL
echo [8/8] Lancement du modele FP-Growth (Paniers)...
start "FP-Growth Model" cmd /k "python fpgrowth.py"
pause