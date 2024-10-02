# IDS706_python_template
This repo contains work for mini-project 1. It sets up an environment on codespaces and uses Github Actions to run a Makefile for the following: `make install`, `make test`, `make format`, `make lint`. 

Some important stuff included are:

* `Makefile`

* `Dockerfile`

* A base set of libraries for devops and web

* `githubactions` 

## Purpose of project
The purpose of this project is to create a python template to use for later projects. To make sure github actions is working properly, I use a Makefile to test various parts of my code. To clarify, I made a simple function in main.py and use various test cases in test_main.py to make sure my code works correctly.

## Preparation
1. open codespaces 
2. wait for container to be built and virtual environment to be activated with requirements.txt installed 

## Check format and test errors 
1. Format code `make format`
2. Lint code `make lint`
