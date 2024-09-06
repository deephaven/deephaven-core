#!/bin/bash

protoc -I/usr/include -I/protos --doc_out=/out `find /protos/ -name \*.proto`