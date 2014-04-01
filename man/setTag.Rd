\name{setTag}
\alias{setTag}
\title{
setTag
}
\description{
Set a worker 'tag' reported to the \code{getTask} function.
}
\usage{
setTag(label)
}
\arguments{
  \item{label}{ A character label.  }
}
\details{
Workers report their tags to the \code{getTask} function. They are ignored by
this function by default. Use custom tags in combination with custom Redis
server-side Lua scripts to define custom task-pulling behavior.
}
\value{
Nothing is returned.
}
\author{
B. W. Lewis <blewis@illposed.net>
}
