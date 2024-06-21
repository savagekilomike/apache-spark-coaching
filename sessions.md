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

# Session 13 (Bonus Session, Testimonial 2024-06-19)

## What we did

- Walk through performance slides: Skew, Spill, Shuffle, Storage, Serialization
- Jupyter notebook and explore Spark UI metrics to detect performance issues
- Record a testimonial

## Assignments

- review the performance bottleneck jupyter notebook
- you can find the Jupyter docker shell script and the notebook in the `jupyter` directory
- download the [generated dataset](https://drive.google.com/drive/folders/1gP7MHbwCiEllYp8nQkfDRs4_fClcpLFJ?usp=sharing) and place it in the `data/gen` directory in your project (the Jupyter docker container will mount the `data` directory to `/data`)
- run the Jupyter docker container moving into the `jupyter` directory and running `./run-jupyter.sh`
- find the WebUI URL from the container logs (contains a token parameter) and open it in your browser
