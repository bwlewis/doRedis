#' A Redis parallel back end for foreach.
#'
#' The doRedis package imlpements an elastic parallel back end
#' for foreach using the Redis key/value database.
#'
#' @name doRedis-package
#'
#' @useDynLib doRedis, .registration=TRUE, .fixes="C_"
#' @seealso \code{\link{registerDoRedis}}, \code{\link{startLocalWorkers}}
#' @docType package
NULL

.doRedisGlobals <- new.env(parent=emptyenv())

REDIS_MAX_VALUE_SIZE <- 524288000
