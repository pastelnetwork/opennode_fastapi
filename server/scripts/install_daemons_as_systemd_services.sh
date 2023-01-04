echo "Now copying the systemd services to /etc/systemd/system/"
cp /home/ubuntu/pastel_mining_monitor/server/units/*.service /etc/systemd/system/
echo "Now starting the systemd services..."
sudo systemctl daemon-reload

sudo systemctl enable database_inserter_daemon.service
sudo systemctl start database_inserter_daemon.service

sudo systemctl enable mining_monitor_daemon.service
sudo systemctl start mining_monitor_daemon.service

sudo systemctl enable data_analysis_daemon.service
sudo systemctl start data_analysis_daemon.service

sudo systemctl enable data_alerting_daemon.service
sudo systemctl start data_alerting_daemon.service

echo "Now checking the status of the systemd services..."
sudo systemctl status mining_monitor_daemon
sudo systemctl status database_inserter_daemon
sudo systemctl status data_analysis_daemon
sudo systemctl status data_alerting_daemon
