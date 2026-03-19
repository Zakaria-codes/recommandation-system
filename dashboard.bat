@echo off
title Lancement du Dashboard Streaming
color 0A

echo =======================================================
echo     LANCEMENT DU DASHBOARD (WINDOWS TERMINAL)
echo =======================================================
echo.

echo Preparation de la Base de Donnees MySQL...
cmd /c "python raw_data.py"
timeout /t 5 /nobreak > NUL

echo Ouverture de l'interface du Dashboard...
:: Lancement de Windows Terminal
wt -M ^
new-tab -p "Command Prompt" --title "Zookeeper" -d . cmd /k "cd C:\kafka && .\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties" ; ^
split-pane -V -p "Command Prompt" --title "Kafka" -d . cmd /k "echo Attente de Zookeeper (20s)... && timeout /t 20 /nobreak > NUL && cd C:\kafka && .\bin\windows\kafka-server-start.bat .\config\server.properties" ; ^
new-tab -p "Command Prompt" --title "Producer" -d . cmd /k "echo Attente de Kafka (35s)... && timeout /t 35 /nobreak > NUL && python producer.py" ; ^
split-pane -V -p "Command Prompt" --title "Consumer" -d . cmd /k "echo Attente du Producer (40s)... && timeout /t 40 /nobreak > NUL && python consumer.py" ; ^
split-pane -H -p "Command Prompt" --title "ALS" -d . cmd /k "echo Attente du flux (45s)... && timeout /t 45 /nobreak > NUL && python als.py" ; ^
move-focus left ; ^
split-pane -H -p "Command Prompt" --title "FP-Growth" -d . cmd /k "echo Attente du flux (45s)... && timeout /t 45 /nobreak > NUL && python fpgrowth.py"

exit