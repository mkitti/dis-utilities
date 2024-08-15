# dis-utilities:api

## Web-based user interface and REST API

### Setup

First, create a Python virtual environment:

    cd api
    python3 -m venv my_venv

Enter the virtual environment and install necessary libraries:

    source my_venv/bin/activate
    venv/bin/pip install -r requirements.txt

Start the development server:

    venv/bin/python3 dis_responder.py

### Running on production

dis.int.janelia.org is the production server for both the MongoDB database and the UI/API. First, you'll need to access that server:

    ssh dis.int.janelia.org

Go to the run directory (be sure to get any changes from the repo):

    cd /opt/flask/dis-utilities
    git pull

Start the server:

    sh restart_production.sh
