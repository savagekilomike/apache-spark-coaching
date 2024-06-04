# Session 11 (2024-05-23)

## What we did

- talk about delta lake as data source
- using SQL vs. DataFrame API to analyze data
- optimize your code for runtime by reducing the number of actions called
  - calculate aggregations for all columns simultaneously (outside of the column loop)
  - how to retrieve results from a single row, using `df.first().asDict()`
- how to pass a dictionary into a query as parameter

# Session 12 (2024-06-03)

## What we did

- talk about your work assignment
- introduction to AWS deployment
- please find the scripts in the `scripts` directory
- please find the documentation in the `aws-deployment.md` file

## Assignments

- walk through the AWS documentation
- create the required resources and run your application on the EMR cluster
- verify that results are created 