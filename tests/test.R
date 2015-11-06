require(doRedis)

compare <- function(x,y,label="unexpected result")
{
  if(!isTRUE(all.equal(x,y))) stop(label)
}

if(Sys.getenv("TEST_DOREDIS") == "TRUE")
{
# Basic test with two local worker processes
  queue <- "jobs"
  redisConnect()
  removeQueue(queue)
  startLocalWorkers(n=2, queue, timeout=5)
  registerDoRedis(queue)
  ans <- foreach(j=1:10, .combine=sum) %dopar% j
  compare(ans,55, "foreach")

# setX tests
  setChunkSize(5)
  x <- 0
  setExport("x")
  setPackages("rredis")
  ans <- foreach(j=1:10, .combine=sum, .noexport="x") %dopar% {
    j + x
  }
  compare(ans, 55, "foreach")

# Shut down
  removeQueue(queue)
}
