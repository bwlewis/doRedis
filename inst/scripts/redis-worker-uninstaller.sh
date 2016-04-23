#!/bin/bash
#
# Stop the doRedis service and remove the files:
# /etc/init.d/doRedis
# /usr/local/bin/doRedis
# /etc/doRedis.conf
#
# Usage:
# sudo ./redis-worker-installer.sh
# sudo ./redis-worker-uninstaller.sh   # (to remove)
#
# On AWS EC2 systems, use:
# sudo ./redis-worker-installer.sh EC2

echo "Un-installing the doRedis service..."
/etc/init.d/doRedis stop
if test -n "`which update-rc.d 2>/dev/null`"; then
  update-rc.d -f doRedis remove
else
  chkconfig --del doRedis
fi
rm -f /etc/init.d/doRedis
rm -f /etc/doRedis.conf
rm -f /usr/local/bin/doRedis
