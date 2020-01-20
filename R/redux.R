#' Internal redux compatability functions with old rredis package
#' @importFrom redux hiredis
#' @param host character Redis host name
#' @param port integer Redis port number
#' @param password optional character Redis password
#' @param ... optional additional arguments for compatability with old rredis, ignored
redisConnect <- function(host="localhost", port=6379L, password, ...)
{
  r <- hiredis(host=host, port=port)
  if(!missing(password)) r$AUTH(password)
  assign("r", r, .doRedisGlobals)
  assign("host", host, .doRedisGlobals)
  assign("port", port, .doRedisGlobals)
  NULL
}

redisGetContext <- function()
{
  .doRedisGlobals
}

redisExists <- function(key)
{
  .doRedisGlobals$r$EXISTS(key) == 1
}

redisSet <- function(key, val)
{
  .doRedisGlobals$r$SET(key, serialize(val, NULL))
}

redisDelete <- function(key)
{
  .doRedisGlobals$r$DEL(key)
}

redisGet <- function(key)
{
  unserialize(.doRedisGlobals$r$GET(key))
}

redisSetPipeline <- function(value)
{
  invisible()  # ignore this for now
}

redisGetResponse <- function(all)
{
  invisible()  # ignore this for now
}

redisMulti <- function()
{
  .doRedisGlobals$r$MULTI()
}

redisExec <- function()
{
  .doRedisGlobals$r$EXEC()
}

redisRPush <- function(key, value, ...)
{
  .doRedisGlobals$r$RPUSH(key, serialize(value, NULL))
}

redisLPush <- function(key, value, ...)
{
  .doRedisGlobals$r$LPUSH(key, serialize(value, NULL))
}

redisBRPop <- function(keys, timeout=0, ...)
{
  x <- .doRedisGlobals$r$BRPOP(keys, timeout=timeout)
  if (length(x) > 1) {
      n <- x[[1]]
      x <- list(unserialize(x[[2]]))
      names(x) <- n
  }
  x
}

redisBLPop <- function(keys, timeout=0, ...)
{
  x <- .doRedisGlobals$r$BLPOP(keys, timeout=timeout)
  if (length(x) > 1) {
      n <- x[[1]]
      x <- list(unserialize(x[[2]]))
      names(x) <- n
  }
  x
}

redisKeys <- function(pattern = "*")
{
  unlist(.doRedisGlobals$r$KEYS(pattern))
}

redisMGet <- function(keys, ...)
{
  x <- .doRedisGlobals$r$MGET(keys)
  names(x) <- if (length(x) == length(keys)) keys else NULL
  x
}

redisLLen <- function(key)
{
  as.integer(.doRedisGlobals$r$LLEN(key))
}

redisLRange <- function (key, start, end, ...)
{
  Map(unserialize, .doRedisGlobals$r$LRANGE(key, start, end))
}

redisIncr <- function (key)
{
  .doRedisGlobals$r$INCR(key)
}

redisDecr <- function (key)
{
  .doRedisGlobals$r$DECR(key)
}

redisExpire <- function (key, seconds)
{
  .doRedisGlobals$r$EXPIRE(key, seconds)
}

redisClose <- function(e)
{
  invisible()
}
