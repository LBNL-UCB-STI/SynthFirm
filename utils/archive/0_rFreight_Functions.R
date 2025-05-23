#' Load a set of files to objects
#'
#' Loads a set of inputs files to objects in the global environment
#' @param filelist list of strings containing file names; uses the names of the list as the object names
#' @param inputdir file path to the inputs directory (e.g. model$inputdir)
#' @keywords Management
#' @export
loadInputs <- function (filelist,inputdir) {
  if(length(filelist)>0){
    for (i in 1:length(filelist)){
      assign(names(filelist)[i],fread(file.path(inputdir,filelist[[i]])),envir = .GlobalEnv)
    }
  }
}


#' Install and load required R packages
#'
#' Load required R packaging, first testing whether a package is 
#' available and installs without popping up the Cran mirror list.
#' @param package Name of R package to load/download and install (character string)
#' @keywords Management
#' @export
loadPackage <- function (package) {
  if(!package %in% .packages(all = TRUE)) {
    install.packages(package, repos = "http://cran.r-project.org")
  }
  eval(parse(text=paste("library(", package, ")", sep="")))
}


#' Convert NAICS 6 to top level 2 digit codes
#'
#' Given naics6 codes (Census, not BEA IO), returns naics2 codes 
#' with some aggregation (e.g. 31,32,33 reclassed as 31 Manufacturing)
#' @param naics6 vector of naics6 codes (Census, not BEA IO)
#' @keywords Freight-Data
#' @export

naics6naics2 <- function(naics6){
  
  #make 2 digit NAICS, recode some values which belong to same broad category
  naics2 <- as.integer(substr(naics6,1,2))
  naics2[naics2 %in% c(32,33)] <- 31L
  naics2[naics2 == 45] <- 44L
  naics2[naics2 == 49] <- 48L
  return(naics2)
}


#source(file.path(basedir,"scripts","predict_logit.R"), echo = TRUE)
#source(file.path(basedir,"scripts","progressEnd.R"), echo = TRUE)
#source(file.path(basedir,"scripts","progressManager.R"), echo = TRUE)
#source(file.path(basedir,"scripts","progressNextStep.R"), echo = TRUE)
#source(file.path(basedir,"scripts","progressStart.R"), echo = TRUE)
#source(file.path(basedir,"scripts","runPMG.R"), echo = TRUE)
source("./scripts/saveOutputs.R", echo = TRUE)
source("./scripts/saveSummary.R", echo = TRUE)
source("./scripts/startModel.R", echo = TRUE)
#source(file.path(basedir,"scripts","writePMGini.R"), echo = TRUE)
