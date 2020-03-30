#!/bin/bash
rm -f /tmp/.X1-lock
killall -9 Xvfb
Xvfb :1 &
sleep 2
export DISPLAY=:1
sleep 2
cd /home/ubuntu/darknet
java -jar EC2Controller.jar
