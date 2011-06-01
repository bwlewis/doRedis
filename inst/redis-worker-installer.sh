#!/bin/bash
#
# This is an experimental script that sets up R doRedis workers to start
# automatically on LSB systems. It installs three files:
# /etc/init.d/doRedis
# /usr/local/bin/doRedis_worker
# /etc/doRedis.conf
#
# and configures the doRedis init script to start in the usual runlevels.
# Edit the /etc/doRedis.conf file to point to the right Redis server and
# to set other configuration parameters.

echo "Installing /etc/init.d/doRedis script..."
cat > /etc/init.d/doRedis <<1ZZZ
#! /bin/sh
### BEGIN INIT INFO
# Provides:          doRedis
# Required-Start:    \$remote_fs \$syslog
# Required-Stop:     \$remote_fs \$syslog
# Default-Start:     2 3 4 5
# Default-Stop:      0 1 6
# Short-Description: doRedis
# Description:       Redis-coordinated concurrent R processes.
### END INIT INFO

# Author: B. W. Lewis <blewis@illposed.net>

# PATH should only include /usr/* if it runs after the mountnfs.sh script
PATH=/sbin:/usr/sbin:/bin:/usr/bin:/usr/local/bin
DESC="Redis-coordinated concurrent R processes."
NAME=doRedis
DAEMON=/usr/local/bin/doRedis_worker
DAEMON_ARGS=/etc/doRedis.conf
PIDFILE=/var/run/doRedis.pid
SCRIPTNAME=/etc/init.d/doRedis

# Load the VERBOSE setting and other rcS variables
. /lib/init/vars.sh

# Define LSB log_* functions.
# Depend on lsb-base (>= 3.0-6) to ensure that this file is present.
. /lib/lsb/init-functions

#
# Function that starts the daemon/service
#
do_start()
{
  sudo -u nobody /usr/local/bin/doRedis_worker /etc/doRedis.conf >/dev/null 2>&1 &
}

#
# Function that stops the daemon/service
#
do_stop()
{
  killall doRedis_worker
}


case "\$1" in
  start)
	log_daemon_msg "Starting Redis worker service"
	do_start
	;;
  stop)
	log_daemon_msg "Stopping Redis worker service"
	do_stop
	;;
  status)
       status_of_proc /user/local/bin/doRedis_worker "doRedis_worker" && exit 0 || exit 1
       ;;
  *)
	echo "Usage: doRedis {start|stop|status}" >&2
	exit 3
	;;
esac

1ZZZ

echo "Installing /usr/local/bin/doRedis_worker helper script..."
cat > /usr/local/bin/doRedis_worker << 2ZZZ
#!/bin/bash
# doRedis R worker startup script

CONF=\$1

[ ! -x \$CONF ]  || echo "Can't find configuration file doRedis.conf, exiting"
[ ! -x \$CONF ] || exit 1

N=\$(cat \$CONF | sed -n /^n:/p | sed -e "s/.*:[[:blank:]*]//")
R=\$(cat \$CONF | sed -n /^R:/p | sed -e "s/.*:[[:blank:]*]//")
T=\$(cat \$CONF | sed -n /^timeout:/p | sed -e "s/.*:[[:blank:]*]//")
HOST=\$(cat \$CONF | sed -n /^host:/p | sed -e "s/.*:[[:blank:]*]//")
PORT=\$(cat \$CONF | sed -n /^port:/p | sed -e "s/.*:[[:blank:]*]//")
QUEUE=\$(cat \$CONF | sed -n /^queue:/p | sed -e "s/.*:[[:blank:]*]//")

Terminator ()
{
  for j in \$(jobs -p); do
    kill \$j
  done
  exit
}
trap "Terminator" SIGHUP SIGINT SIGTERM

while true; do
  j=0
  while test \$j -lt \$N; do
    \${R} --slave -e "require('doRedis'); tryCatch(redisWorker(queue=\"\${QUEUE}\", host=\"\${HOST}\", port=\${PORT},timeout=\${T}),error=function(e) q(save='no'));q(save='no')" >/dev/null 2>&1 &
    j=\$((\$j + 1))
  done
  wait
done
2ZZZ

chmod +x /usr/local/bin/doRedis_worker

echo "Installing /etc/doRedis.conf configuration file                                   (you probably want to edit this)..."
cat > /etc/doRedis.conf << 3ZZZ
# Set n to the number of workers to start.
n: 1
# Set R to the path to R (default assumes 'R' is in the PATH)
R: R
timeout: 5
host: localhost
port: 6379
queue: RJOBS

3ZZZ


chmod a+x /etc/init.d/doRedis
update-rc.d doRedis defaults
/etc/init.d/doRedis start
