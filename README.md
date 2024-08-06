# dis-utilities [![Picture](https://raw.github.com/janelia-flyem/janelia-flyem.github.com/master/images/HHMI_Janelia_Color_Alternate_180x40.png)](http://www.janelia.org)

[![GitHub last commit](https://img.shields.io/github/last-commit/JaneliaSciComp/dis-utilities.svg)](https://github.com/JaneliaSciComp/dis-utilities)
[![GitHub commit merge status](https://img.shields.io/github/commit-status/badges/shields/master/5d4ab86b1b5ddfb3c4a70a70bd19932c52603b8c.svg)](https://github.com/JaneliaSciComp/dis-utilities)

[![Python](https://img.shields.io/badge/Python-FFD43B?style=for-the-badge&logo=python&logoColor=blue)](https://www.python.org/)
[![MongoDB](https://img.shields.io/badge/MongoDB-4EA94B?style=for-the-badge&logo=mongodb&logoColor=white)](https://www.mongodb.com/)
[![MySQL](https://img.shields.io/badge/MySQL-73618F?style=for-the-badge&logo=mysql&logoColor=white)](https://www.mysql.com/)
[![Docker](https://img.shields.io/badge/Docker-2CA5E0?style=for-the-badge&logo=docker&logoColor=white)](https://www.docker.com/)

## Utilities for Data and Information Services

This repository is split into three sections:

- [api](api/README.md): Web-based user interface and REST API
- [sync](sync/README.md): programs meant to be periodically run in the backgroud to sync the DIS database from external data sources
- [utility](utility/README.md): utility programs

## DIS system architecture
![DIS system architecture](DIS_architecture.png?raw=true "DIS system architecture")

The DIS system is based on a MongoDB database with collections to persist DOIs, ORCIDs, and project mappings. Python programs are used for ETL and updates. A Flask-based application provides user interface, visualizations, and a REST API.
