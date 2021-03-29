# Overview

Welcome to the Step-X repository. This repo is dedicated to the data extraction and manipulation of the World Bank's database called STEP. Bellow in this readme, it will be explained the installation and usage process.

![](images/stepX_logo.jpg)


The extractor was created using the following technologies:

- Python 3.8
- Pandas
- Geeckodriver
- Selenium
- MongoDB


## Installation process

To install and prepare the Step-X environment it's necessary to follow these instructions in order, step by step. To start, it's needed to:


- Install the [Geckodriver](docs/geckodriver_install.md) 
- Install the [Firefox](docs/firefox_install.md) web browser
- Install [Anaconda](https://docs.anaconda.com/anaconda/install/linux/) and create an environment to proceed with the next steps (if you wish, you can skip this step)
- Install [MongoDB](https://docs.mongodb.com/manual/tutorial/install-mongodb-on-ubuntu/) in your machine or server

Once installed the required tools describe above, we need to install the Python's libraries used in this project. To make that, execute the command below:

    conda create --name <env> --file requirements.txt

This command installs the libraries and create a new conda environment. After that, your workspace is prepared to execute the extractor, but you will need to follow some configuration instructions that will be described in the next steps.


## Configuration process

To start the extraction, first some configurations is required, such as the World Bank's credentials and the project list that the extractor will retrieve data. Notice that all necessary configuration is imbued in the file called environment.py. To set the World Bank's credentials just replace the variable called wb_credentials with the correct credentials as the example bellow:

    wb_credentials = {"email": 'email@test.com', 'password': 'password123'}

The geckodriver path is also needed to ensure that the Selenium will be work properly. To set the geckodriver path, just replace the variable geckodriver_path with the desired location:

    geckodriver_path = r'/Users/userName/webdriverLocationFolder/geckodriver'

The next step is to set up the database credentials pass name, and the url in environment.py as the example bellow:

    database_name = "stepX"
    database_url = "localhost"

Finally, for the last configuration, pass the project's list that you wish to extract and manipulate. Follow the example:

    PROJECTS_LIST =['PROJECT_ID']

