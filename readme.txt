This demo project utilizes multiprocessing and asyncio for web crawling, specifically crawling for email addresses.

Pre-requirements:
1. network connection
2. docker and docker compose installed

How to run the crawler:
1. Change the parameters in the config file as required,  add the initial seed sites.
2. Change parameters of the docker-compose for your deployment:
    a. You will probably want to change the mounting location of neo4j's persistent data.
    b. Config file for the crawler.
    * The crawler assumes that there is a config file mounted to /config/config.yaml.
2. Run docker compose up via command
    docker-compose up

* If it is the initial run you will need to build the crawler's docker file-this can be done with the command:
    docker-compose up --build