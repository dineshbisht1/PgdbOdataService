# PgdbOdataService
Run a Odata server to perform CRUD operation on a Postgres database
Server.py exposes an OData endpoint, which provides access to a Postgres database.
This directory contains scripts that take in the XML file exposed at
`/$metadata` describing the database’s schema and starts a REST based server 
enabling CRUD operation on the Postgres database.
Metadata.xml contains schema details of Postgress database and needs to be generated manually.

## Usage
The command line interface to these scripts takes one arguments; the name of the config file
containing connection details for http server, pgdb and metadata xml file location.
