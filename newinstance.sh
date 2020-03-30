sudo apt install default-jre
sudo apt install default-jdk
crontab -l | { cat; echo "@reboot /home/ubuntu/darknet/./startup.sh > cronrun.log"; } | crontab -
mkdir ~/.aws
echo "newfile" >  ~/.aws/credentials
cd  darknet/
echo "newfile" >  EC2Controller.jar
echo "newfile" >  startup.sh
chmod u+x startup.sh
echo "newfile" >  cronrun.log
  
