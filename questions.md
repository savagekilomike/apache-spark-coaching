# Open questions
- source files of apache spark documentation
- Spark vs. Pandas
    - capabilities
    - performance
- how to add sorting by month in 'MMM' format?

```python
df.groupby(year(df['date']).alias("year"),  \
           date_format(df['date'], 'MMM').alias("month"))    \
            .agg(max("close").alias("max_close"), avg("close").alias("avg_close"))    \
            .sort(ax("close"))             \
            .show()
```

- abstractions behind the documentation?
- what is Spark Connect?
