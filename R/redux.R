#' Explicitly connect to a Redis server.
#'
#' This function is normally not needed, use the redux package functions instead,
#' or simply registerDoRedis.
#' @importFrom redux hiredis
#' @param host character Redis host name
#' @param port integer Redis port number
#' @param password optional character Redis password
#' @param ... optional additional arguments for compatability with old rredis, ignored
#' @seealso \code{\link{registerDoRedis}}, \code{\link{redisWorker}}, \code{\link{startLocalWorkers}}
#' @export
redisConnect <- function(host="localhost", port=6379L, password, ...)
{
  .doRedisGlobals$r = NULL
  r <- hiredis(host=host, port=port)
  if(!missing(password)) r$AUTH(password)
  assign("r", r, .doRedisGlobals)
  assign("host", host, .doRedisGlobals)
  assign("port", port, .doRedisGlobals)
  NULL
}

checkConnect <- function()
{
  if(is.null(.doRedisGlobals$r))
    stop("Connect to redis first with `redisConnect`, `startLocalWorkers` or `redisWorker`.")
}

redisGetContext <- function()
{
  checkConnect()
  .doRedisGlobals
}

uncerealize <- function(x)
{
  if(!is.null(x) && is.raw(x)) unserialize(x) else NULL
}

redisExists <- function(key)
{
  checkConnect()
  .doRedisGlobals$r$EXISTS(key) == 1
}

#' A convenience function to set an R value in a Redis key
#'
#' This function serializes the val argument.
#'
#' @param key (character or raw) Redis key name
#' @param val R value to set
#' @return Redis status message
#' @seealso \code{\link{hiredis}}
#' @export
redisSet <- function(key, val)
{
  checkConnect()
  .doRedisGlobals$r$SET(key, serialize(val, NULL))
}

#' A convenience function to delete a Redis key
#'
#' @param key (character or raw) Redis key name to delete
#' @return Redis status message
#' @seealso \code{\link{hiredis}}
#' @export
redisDelete <- function(key)
{
  checkConnect()
  .doRedisGlobals$r$DEL(key)
}

#' A convenience function to return an R value from a Redis key.
#'
#' This function assumes the value associated with the Redis key
#' is a serialized (binary) R value and unserializes it on return.
#'
#' @param key (character or raw) Redis key name
#' @return Unserialized R value.
#' @seealso \code{\link{hiredis}}
#' @export
redisGet <- function(key)
{
  checkConnect()
  uncerealize(.doRedisGlobals$r$GET(key))
}

redisSetPipeline <- function(value)
{
  checkConnect()
  invisible()  # ignore this for now
}

redisGetResponse <- function(all)
{
  checkConnect()
  invisible()  # ignore this for now
}

redisMulti <- function()
{
  checkConnect()
  .doRedisGlobals$r$MULTI()
}

redisExec <- function()
{
  checkConnect()
  .doRedisGlobals$r$EXEC()
}

redisRPush <- function(key, value, ...)
{
  checkConnect()
  .doRedisGlobals$r$RPUSH(key, serialize(value, NULL))
}

redisLPush <- function(key, value, ...)
{
  checkConnect()
  .doRedisGlobals$r$LPUSH(key, serialize(value, NULL))
}

redisBRPop <- function(keys, timeout=0, ...)
{
  checkConnect()
  x <- .doRedisGlobals$r$BRPOP(keys, timeout=timeout)
  if (length(x) > 1) {
      n <- x[[1]]
      x <- list(uncerealize(x[[2]]))
      names(x) <- n
  }
  x
}

redisBLPop <- function(keys, timeout=0, ...)
{
  checkConnect()
  x <- .doRedisGlobals$r$BLPOP(keys, timeout=timeout)
  if (length(x) > 1) {
      n <- x[[1]]
      x <- list(uncerealize(x[[2]]))
      names(x) <- n
  }
  x
}

redisKeys <- function(pattern = "*")
{
  checkConnect()
  unlist(.doRedisGlobals$r$KEYS(pattern))
}

redisMGet <- function(keys, ...)
{
  checkConnect()
  x <- Map(uncerealize, .doRedisGlobals$r$MGET(keys))
  names(x) <- if (length(x) == length(keys)) keys else NULL
  x
}

redisLLen <- function(key)
{
  checkConnect()
  as.integer(.doRedisGlobals$r$LLEN(key))
}

redisLRange <- function (key, start, end, ...)
{
  checkConnect()
  Map(uncerealize, .doRedisGlobals$r$LRANGE(key, start, end))
}

redisIncr <- function (key)
{
  checkConnect()
  .doRedisGlobals$r$INCR(key)
}

redisDecr <- function (key)
{
  checkConnect()
  .doRedisGlobals$r$DECR(key)
}

redisExpire <- function (key, seconds)
{
  checkConnect()
  .doRedisGlobals$r$EXPIRE(key, seconds)
}

redisClose <- function(e)
{
  .doRedisGlobals$r = NULL
}
