#' Reconnect to redis
#' @return \code{NULL}
#' @keywords internal
reconnect <- function()
{
  con <- redisGetContext()
  redisConnect(host=con$host, port=con$port, nodelay=con$nodelay)
}
