#' Reconnect to redis
#' @return \code{NULL}
#' @keywords internal
#' @importFrom rredis redisGetContext redisConnect
reconnect <- function()
{
  con <- redisGetContext()
  redisConnect(host=con$host, port=con$port, nodelay=con$nodelay)
}
