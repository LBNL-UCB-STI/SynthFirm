#progressStart(firmsyn,9)

packages = c(
  "dplyr",
  "dtplyr",
  "data.table",
  "bit64",
  "reshape",
  "reshape2",
  "ggplot2",
  "fastcluster",
  "sf"
)
lapply(packages, require, character = TRUE)