args <- commandArgs(TRUE)

result <- tryCatch({

  if (length(args) < 2) stop("Must provide input and output directories: Rscript --vanilla run-modeling.R /input /output")

  input.directory <- args[[1]]
  output.directory <- args[[2]]

  source("/model/run-modeling-core.R")

  run.modeling(input.directory, output.directory)

}, error = function(e) {

  print(paste("Failed to run modeling", e))
  quit(status = 1)

})
