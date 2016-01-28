run.modeling <- function(
  input.directory,         # directory with input data
  output.directory         # directory to write output model
) {

  print(paste("Run modeling with input directory:", input.directory, "; output directory:", output.directory))

  predictors.path <- file.path(input.directory, "predictors.csv")
  response.path <- file.path(input.directory, "response.csv")
  columns.path <- file.path(input.directory, "columns.csv")

  print("Load dataset")
  dataset <- load.dataset(
    predictors.path,
    response.path,
    columns.path
  )

  print("Train model")
  mdl <- build.glmnet.model(dataset)

  print("Save model coefficients")
  coeff.df <- model.coeffs(mdl, dataset$columns)

  save.file <- file.path(output.directory, "model.csv")
  write.csv(coeff.df, file = save.file, row.names = FALSE)
}

# Load dataset from 3 files
load.dataset <- function(
  predictors.path,
  response.path,
  columns.path
) {
  require(methods)
  require(Matrix)

  # row, column, value
  predictors.csv <- read.csv(predictors.path, header = TRUE)

  # row, response
  response.csv <- read.csv(response.path, header = TRUE)

  # column, name
  columns <- read.csv(columns.path, header = TRUE)

  sparse.matrix = sparseMatrix(
      i = predictors.csv[,1] # row
    , j = predictors.csv[,2] # column
    , x = predictors.csv[,3] # value
  )

  response <- response.csv[,2]

  dataset <- list(
    columns = columns,
    x = sparse.matrix,
    y = response
  )
}

# Train dummy binomial model
build.glmnet.model <- function(
  dataset = dataset,
  alpha = 0.01,
  nfolds = 5
) {
  require (glmnet)

  mdl <- cv.glmnet(
    x = dataset$x,
    y = dataset$y,
    alpha = alpha,
    nfolds = nfolds,
    family = 'binomial',
    type.measure = "deviance"
  )

  return(mdl)
}

# Extract data frame with column names and weights
#  - 0 column associated with slope
model.coeffs <- function(mdl, columns) {

  min.lambda <- which(mdl$lambda == mdl$lambda.min)
  coeff <- c(mdl$glmnet.fit$a0[min.lambda], mdl$glmnet.fit$beta[, min.lambda])

  num.col <- length(coeff)
  coeff.df <- data.frame(
    column = 0:(num.col-1),
    coeff = coeff
  )
  coeff.df <- merge(x = columns, y = coeff.df, by = "column", all.y = TRUE)

  return(coeff.df)
}
