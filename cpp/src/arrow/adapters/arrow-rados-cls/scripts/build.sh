#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set -ex

cd cpp/

if [ ! -d "debug" ]; then

mkdir -p debug
cd debug/
cmake -DCMAKE_BUILD_TYPE=Debug -DARROW_CLS=ON -DARROW_PARQUET=ON -DARROW_WITH_SNAPPY=ON -DARROW_WITH_ZLIB=ON -DARROW_BUILD_EXAMPLES=ON -DPARQUET_BUILD_EXAMPLES=ON -DARROW_PYTHON=ON -DARROW_DATASET=ON -DARROW_CSV=ON -DARROW_WITH_LZ4=ON -DARROW_WITH_ZSTD=ON -DARROW_BUILD_TESTS=ON ..

else

cd debug/

fi

make -j4 install
