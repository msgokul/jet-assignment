-- Create Airflow database and user
CREATE USER airflow WITH SUPERUSER PASSWORD 'airflow';
CREATE DATABASE airflow OWNER airflow;
GRANT ALL PRIVILEGES ON DATABASE airflow TO airflow;

-- xkcd DB
CREATE USER xkcd_user WITH PASSWORD 'xkcd_pass';
CREATE DATABASE xkcd_db OWNER xkcd_user;
GRANT ALL PRIVILEGES ON DATABASE xkcd_db TO xkcd_user;

