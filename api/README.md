# dis-utilities:api

## Web-based user interface and REST API

### Setup

First, create a Python virtual environment:

    cd api
    python3 -m venv my_venv

Enter the virtual environment and install necessary libraries:

    source my_venv/bin/activate
    my_venv/bin/pip install -r requirements.txt

If it doesn't already exist, create the configuration file:

    cp config_template.cfg config.cfg

Change values in config.cfg for this specific installation.

Start the development server:

    my_venv/bin/python3 dis_responder.py

### Running on production

dis.int.janelia.org is the production server for both the MongoDB database and the UI/API. First, you'll need to access that server:

    ssh dis.int.janelia.org

Go to the run directory (be sure to get any changes from the repo):

    cd /opt/flask/dis-utilities
    git pull

If it doesn't already exist, create the configuration file:

    cp config_template.cfg config.cfg

Change values in config.cfg for this specific installation.

If it doesn't already exist, create the Docker compose file:

    cp docker-compose-prod_template.yml docker-compose-prod.yml

Change values in docker-compose-prod.yml for this specific installation.

Start the server:

    sh restart_production.sh
