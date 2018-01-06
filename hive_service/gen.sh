#!/bin/bash
OPTS="package=rpc"
thrift -r --gen go:${OPTS} thrift/TCLIService.thrift 
cp -r gen-go/* ./
rm -rf gen-go
rm -rf rpc/t_c_l_i_service-remote 