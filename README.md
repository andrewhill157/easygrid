# easygrid
This is currently a work in progress to make a simple python-based pipeline execution tool for grid engine distributed computing platforms.

Many pipelining tools such as `Queue`, `snakemake`, etc. provide powerful tools for specifying all inputs and outputs of a tool and automating inference of dependencies. I have found that for many small projects specifying all inputs and outputs becomes quite cumbersome and I wanted something easier to use. 

This tool allows you to add commands and give them a stage name. You can make jobs assigned to a given stage dependent on any list of other stages. easygrid will figure out which order to execute your commands, monitor their status on grid engine, and then report back detailed statistics on their completion status and resource usage.

# Example
```
TODO
```
