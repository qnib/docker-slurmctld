#!/bin/bash

su -c 'touch /scratch/test_alice' alice
if [ $? -eq 1 ];then
   chmod 777 /scratch/
fi
su -c 'touch /scratch/test_alice' alice
if [ $? -eq 1 ];then
   exit 1
fi
su -c 'touch /scratch/test_john' john
if [ $? -eq 1 ];then
   exit 1
fi
rm -rf /scratch/*

