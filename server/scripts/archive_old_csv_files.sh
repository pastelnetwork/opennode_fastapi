echo "Moving output CSV files more than 3 days old to archive directory..."
find /home/ubuntu/pastel_mining_monitor/output_csv_files/ -maxdepth 1 -name "*.csv" -mtime +3 -type f -exec mv "{}" /home/ubuntu/pastel_mining_monitor/archive/ \;
echo "Compressing the moved CSV files..."
7z a /home/ubuntu/pastel_mining_monitor/compressed_archive_files/old_files.7z /home/ubuntu/pastel_mining_monitor/archive/*1.csv
7z a /home/ubuntu/pastel_mining_monitor/compressed_archive_files/old_files.7z /home/ubuntu/pastel_mining_monitor/archive/*2.csv
7z a /home/ubuntu/pastel_mining_monitor/compressed_archive_files/old_files.7z /home/ubuntu/pastel_mining_monitor/archive/*3.csv
7z a /home/ubuntu/pastel_mining_monitor/compressed_archive_files/old_files.7z /home/ubuntu/pastel_mining_monitor/archive/*4.csv
7z a /home/ubuntu/pastel_mining_monitor/compressed_archive_files/old_files.7z /home/ubuntu/pastel_mining_monitor/archive/*5.csv
7z a /home/ubuntu/pastel_mining_monitor/compressed_archive_files/old_files.7z /home/ubuntu/pastel_mining_monitor/archive/*6.csv
7z a /home/ubuntu/pastel_mining_monitor/compressed_archive_files/old_files.7z /home/ubuntu/pastel_mining_monitor/archive/*7.csv
7z a /home/ubuntu/pastel_mining_monitor/compressed_archive_files/old_files.7z /home/ubuntu/pastel_mining_monitor/archive/*8.csv
7z a /home/ubuntu/pastel_mining_monitor/compressed_archive_files/old_files.7z /home/ubuntu/pastel_mining_monitor/archive/*9.csv
7z a /home/ubuntu/pastel_mining_monitor/compressed_archive_files/old_files.7z /home/ubuntu/pastel_mining_monitor/archive/*0.csv
echo "Compression was successful, now deleting original files..."
find /home/ubuntu/pastel_mining_monitor/archive/ -maxdepth 1 -name "*.csv" -delete
echo "Done deleting files!"
