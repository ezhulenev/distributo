# Dummy Model Input Generator

Generate input data for training dummy logistic regression model.

Input data generated in sparse format compatible with `R` `Matrix` package.

### Run 

```sh
lein run --generate --num-negatives 9000 --num-positives 1000 
```

Result will be available in `output` directory.
