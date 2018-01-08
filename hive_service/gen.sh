#!/bin/bash
docker run -it --rm -u"${UID}" -v "${PWD}:/data" -w "/data" thrift:0.9.3 thrift -r --gen go:"package=rpc" thrift/TCLIService.thrift
cp -r gen-go/* ./
rm -rf gen-go
rm -rf rpc/t_c_l_i_service-remote 