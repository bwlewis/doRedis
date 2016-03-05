require(doRedis)
require(rredis)

compare <- function(x, y, label="unexpected result")
{
  if(!isTRUE(all.equal(x, y))) stop(label)
}

# covr can't detect coverage of worker routines run in separate
# processes. This example test explicitly constructs a work queue
# and runs a worker locally to test it.
if(Sys.getenv("TEST_DOREDIS") == "TRUE")
{
  redisConnect()
  redisFlushAll()
  IDfile <- tempfile("doRedis")
  zz <- file(IDfile,"w")
  close(zz)
  ID <- basename(IDfile)
  ID <- gsub("\\\\","_",ID)
  queue <- "jobs2"
  queueEnv <- paste(queue,"env", ID, sep=".")
  queueOut <- paste(queue,"out", ID, sep=".")
  queueStart <- paste(queue,"start",ID, sep=".")
  queueStart <- paste(queueStart, "*", sep="")
  queueAlive <- paste(queue,"alive",ID, sep=".")
  queueAlive <- paste(queueAlive, "*", sep="")
  e <- new.env()
  e$.Random.seed <- as.integer(c(1,2,3))
  redisSet(queueEnv, list(expr=as.symbol("j"), exportenv=e, packages=c()))
  block <- list(`1`=1)
  redisRPush(queue, list(ID=ID, argsList=block))
  redisWorker(queue, iter=1, timeout=1, log=stderr())
  redisConnect()
  redisFlushAll()
}
