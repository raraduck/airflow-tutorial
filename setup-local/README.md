# Airflow Local Install in Ubuntu 22.04

## 1. Install airflow 2.10.5
```bash
sudo pip3 install apache-airflow==2.10.5
```

## 2. Add user
```bash
sudo groupadd airflow
sudo useradd -s /bin/bash airflow -g airflow -d /var/lib/airflow -m # airflow 사용자 작업경로 설정
```
You can check airflow group in /etc/group
```bash
sudo su - airflow
airflow$ pwd
airflow$ mkdir dags
```

## 3. Init database (SQLite as default)
```bash
airflow$ AIRFLOW_HOME=/var/lib/airflow airflow db init
```
check config files and folders
- airflow.cfg
- airflow.db
- dags
- logs

## 4. change to postgresql in root user
```bash
airflow$ exit
root$ sudo apt-get install postgresql15.x86_64 postgresql15-server -y
```
Check version
```bash
root$ /user/bin/postgres --version
# postgres (PostgreSQL) 15.9
```
Init db
```bash
root$ sudo /usr/bin/postgresql-setup --initdb
# Initializing database in '/var/lib/pgsql/data'
# Initialized, logs are in /var/lib/pgsql/initdb_postgrsql.log
```

## 5. Configrue postgres
```bash
root$ vim /var/lib/pgsql/data/postgresql.conf
```
in /var/lib/pgsql/data/postgresql.conf
```bash 
# listen_addresses = 'localhost' 
listen_addresses = '*'
# port = 5432
port = 5432
```
- uncomment and open to all ip address
```bash
service postgresql restart
```
in /var/lib/pgsql/data/pg_hba.conf
```bash
# IPv4 local connections:
# host    all     all     127.0.0.1/32    ident
host    all     airflow     0.0.0.0/0   truest
```
```bash
service postgresql restart
```

## 6. Connection to postgresql
```bash
su - postgres
postgres$ psql
# psql(15.9)
# postgres=#
```
### 6.1. create airflow user and table in postgresql
```sql
-- CREATE ROLE
postgres=# CREATE USER airflow PASSWORD 'airflow';
-- CREATE DATABASE
postgres=# CREATAE DATABASE airflow; 
-- 3) airflow DB에 대한 권한 부여
postgres=# GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;

postgres=# \q
```
restart db
```bash
postgres$ exit
root$ service postgresql restart
```
## 7. configure airflow db env
```bash
root$ sudo su - airflow
airflow$ vim airflow.cfg # in /var/lib/airflow
```
In /var/lib/airflow.cfg
```bash
executor = LocalExecutor # SequentialExecutor
# sql_alchemy_conn = sqlite:///var/lib/airflow/airflow.db
sql_alchemy_conn = postgresql+psycopg2://airflow:airflow@localhost:5432/airflow
load_example = False # True
default_ui_timezone = Asia/Seoul # UTC
default_timezone = Asia/Seoul # utc
```
Restart airflow
```bash
airflow$ AIRFLOW_HOME=/var/lib/airflow airflow db init
# Initialization done
airflow$ exit
root$ 
```
## 8. Enable Airflow Webserver
```bash
root$ vim /etc/systemd/system/airflow-webserver.service
```
In /etc/systemd/system/airflow-webserver.service
```bash
[Unit]
Description=Airflow webserver
After=network.target

[Service]
Environment=AIRFLOW_HOME=/var/lib/airflow
User=airflow
Group=airflow
Type=simple
ExecStart=/usr/local/bin/airflow webserver -p 8080
Restart=on-failure
RestartSec=10s

[Install]
WantedBy=multi-user.target
```
```bash
root$ vim /etc/systemd/system/airflow-scheduler.service
```
In /etc/systemd/system/airflow-scheduler.service
```bash
[Unit]
Description=Airflow scheduler
After=network.target

[Service]
Environment=AIRFLOW_HOME=/var/lib/airflow
User=airflow
Group=airflow
Type=simple
ExecStart=/usr/local/bin/airflow scheduler
Restart=on-failure
RestartSec=10s

[Install]
WantedBy=multi-user.target
```
```bash
root$ sudo systemctl daemon-reload
root$ sudo systemctl enable airflow-webserver
root$ sudo systemctl enable airflow-scheduler
root$ sudo systemctl start airflow-webserver
root$ sudo systemctl start airflow-scheduler
root$ sudo systemctl status airflow-webserver
root$ sudo systemctl status airflow-scheduler
root$ sudo su - airflow
airflow$ AIRFLOW_HOME=/var/lib/airflow airflow users create --role Admin --username admin --email admin --firstname admin --lastname admin --password admin
# User "admin" created with role "Admin"
```
Access to http://<publicIP>:8080 with 8080 port opened in Security Group.


