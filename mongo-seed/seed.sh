#!/bin/bash

echo "Seeding database from existing dump..."

mkdir /tmp/seeding
cp /docker-entrypoint-initdb.d/dump.tar.bz2 /tmp/seeding
cd /tmp/seeding
tar xvjpf dump.tar.bz2
mongorestore
