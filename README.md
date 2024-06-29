# SF-Data
This Repo Contains all the work related to Salesforce Data 


/*****    Bulk APi        ****/

This file has code to replicate Salesforce Data into SQL SERVER.

Steps to execute the package :

1) Clone BulkApi.py and Config.ini
2) Install python in your System
3) Install following packages. Can run below commands from command Prompt
        python -m pip install pandas
        python -m pip install simple-salesforce 
        python -m pip install sqlalchemy  
        python -m pip install  configparser 
        python -m pip install  pyodbc
        python -m pip install  datetime
        python -m pip install  numpy

4) Set up your Config.ini file with your salesforce and Data base credintials
5) Run your python package with below command. Make Sure your terminal points to Same folder.
6) other wise use CD command to set the path

     python BulkApi.py   -- command to execute code
      

