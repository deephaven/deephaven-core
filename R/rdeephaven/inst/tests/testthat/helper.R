get_dh_target <- function() {
  dh_host <- Sys.getenv("DH_HOST")
  if (dh_host == "") {
    dh_host <- "localhost"
  }
  dh_port <- Sys.getenv("DH_PORT")
  if (dh_port == "") {
    dh_port <- 10000
  }
  return(paste0(dh_host, ":", dh_port))
}
