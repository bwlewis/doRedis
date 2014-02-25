# Default task pulling function. Pull a task from job id job_id on job queue
# queue. All _getTask functions must follow the following convention:
#
# Function arguments must include at least:
# queue (characer) name of job queue
# id (character) job ID
#
# The function must return a task list, an R list with the following
# fields:
# task_id (character) task ID
# args (list) expression arguments


default_getTask <-  function(queue, job_id, ...)
{
  key <- sprintf("%s:%s",queue, job_id)
  redisEval("local x=redis.call('hkeys',KEYS[1])[1];if x==nil then return nil end;local ans=redis.call('hget',KEYS[1],x);redis.call('hdel',KEYS[1],x);return ans",key)
} 
