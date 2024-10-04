## SQLite Lab

![4 17-etl-sqlite-RAW](https://github.com/nogibjj/sqlite-lab/assets/58792/b39b21b4-ccb4-4cc4-b262-7db34492c16d)



### Lab:

* Use an AI Assistant, but use a different one then you used from a previous lab (Anthropic's Claud, Bard, Copilot, CodeWhisperer, Colab AI, etc)
* ETL-Query:  [E] Extract a dataset from URL, [T] Transform, [L] Load into SQLite Database and [Q] Query
For the ETL-Query lab:
* [E] Extract a dataset from a URL like Kaggle or data.gov. JSON or CSV formats tend to work well.
* [T] Transform the data by cleaning, filtering, enriching, etc to get it ready for analysis.
* [L] Load the transformed data into a SQLite database table using Python's sqlite3 module.
* [Q] Write and execute SQL queries on the SQLite database to analyze and retrieve insights from the data.

#### Tasks:

* Fork this project and get it to run
* Make the query more useful and not a giant mess that prints to screen
* Convert the main.py into a command-line tool that lets you run each step independantly
* Fork this project and do the same thing for a new dataset you choose
* Make sure your project passes lint/tests and has a built badge
* Include an architectural diagram showing how the project works

#### Reflection Questions

* What challenges did you face when extracting, transforming, and loading the data? How did you overcome them?
* What insights or new knowledge did you gain from querying the SQLite database?
* How can SQLite and SQL help make data analysis more efficient? What are the limitations?
* What AI assistant did you use and how did it compare to others you've tried? What are its strengths and weaknesses?
* If you could enhance this lab, what would you add or change? What other data would be interesting to load and query?

##### Challenge Exercises

* Add more transformations to the data before loading it into SQLite. Ideas: join with another dataset, aggregate by categories, normalize columns.
* Write a query to find correlated fields in the data. Print the query results nicely formatted.
* Create a second table in the SQLite database and write a join query with the two tables.
* Build a simple Flask web app that runs queries on demand and displays results.
* Containerize the application using Docker so the database and queries can be portable


