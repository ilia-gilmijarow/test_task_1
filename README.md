# Programming Exercise using PySpark

## Background:
A very small company called **KommatiPara** that deals with bitcoin trading has two separate datasets dealing with clients that they want to collate to starting interfacing more with their clients. One dataset contains information about the clients and the other one contains information about their financial details.

The company now needs a dataset containing the emails of the clients from the United Kingdom and the Netherlands and some of their financial details to starting reaching out to them for a new marketing push.

## Usage
Simply call the program with three parameters: the paths to input data files as the first two and a comma-separated list of filtered countries as the third enclose the arguments in quotes or escape spaces.
The program will use PySpark to
- read inputs
- remove innecessary columns
- filter out rows with relevant countries
- combine data sets
- give friendly names to columns
- save output file.

If too few inputs are provided the program exits with a message instructing you about this fact.