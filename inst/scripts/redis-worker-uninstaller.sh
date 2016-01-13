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
#
# Usage:
# sudo ./redis-worker-installer.sh
# sudo ./redis-worker-uninstaller.sh   # (to remove)
#
# On AWS EC2 systems, use:
# sudo ./redis-worker-installer.sh EC2

echo "Un-installing the doRedis service..."
/etc/init.d/doRedis stop
update-rc.d -f doRedis remove || chkconfig --del doRedis
rm -f /etc/init.d/doRedis
rm -f /etc/doRedis.conf
