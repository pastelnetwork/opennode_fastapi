DATE=$(date '+%Y_%m_%d')

echo "Now backing up database. First stopping daemon systemd services..."
sudo systemctl stop mining_monitor_daemon.service
sudo systemctl stop database_inserter_daemon.service
sudo systemctl stop data_analysis_daemon.service
sudo systemctl stop data_alerting_daemon.service

echo "Now backing up database..."
sqlite3 /home/ubuntu/opennode_fastapi/db/opennode_fastapi.sqlite ".timeout 2000" ".backup /home/ubuntu/db_backups/opennode_fastapi__backup__$DATE.sqlite"
echo "Done backing up database, now restarting daemon systemd services..."
sudo systemctl start mining_monitor_daemon.service
sudo systemctl start database_inserter_daemon.service
sudo systemctl start data_analysis_daemon.service
sudo systemctl start data_alerting_daemon.service
