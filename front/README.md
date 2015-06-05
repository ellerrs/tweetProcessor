# README #

### Environment Setup ###

sudo apt-get install python-pip virtualenv build-essential postgresql postgresql-contrib python-dev
sudo apt-get build-dep python-psycopg2

virtualenv env

/ add to activate
PSQL_PASSWORD="<password>"
export PSQL_PASSWORD
PSQL_USER="ironkip"
export PSQL_USER
SESSION_KEY=''
export SESSION_KEY
SECRET_KEY=''
export SECRET_KEY

. env/bin/activate

pip install -r requirements.txt    # generated from pip freeze > requirements.txt

sudo adduser ironkip
sudo -i -u postgres
createuser --interactive  (create the ironkip user)
psql
ALTER USER ironkip WITH PASSWORD '<password>';


### Who do I talk to? ###

* Shane Eller: shane.eller@gmail.com
