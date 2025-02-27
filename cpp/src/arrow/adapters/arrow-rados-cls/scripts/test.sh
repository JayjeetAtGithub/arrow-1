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

set -x

build_dir=cpp/debug
test_dir=${build_dir}/test-cluster

pushd ${build_dir}

    # get rid of process and directories leftovers
    pkill ceph-mon || true
    pkill ceph-osd || true
    rm -fr ${test_dir}

    # cluster wide parameters
    mkdir -p ${test_dir}/log
    cat >> /etc/ceph/ceph.conf <<EOF
[global]
fsid = $(uuidgen)
osd crush chooseleaf type = 0
run dir = ${test_dir}/run
auth cluster required = none
auth service required = none
auth client required = none
osd pool default size = 1
EOF
    export CEPH_ARGS="--conf /etc/ceph/ceph.conf"

    # start a MON daemon
    MON_DATA=${test_dir}/mon
    mkdir -p $MON_DATA

    cat >> /etc/ceph/ceph.conf <<EOF
[mon.0]
log file = ${test_dir}/log/mon.log
chdir = ""
mon cluster log file = ${test_dir}/log/mon-cluster.log
mon data = ${MON_DATA}
mon addr = 127.0.0.1
# this was added to enable pool deletion within method delete_one_pool_pp()
mon_allow_pool_delete = true
EOF

    ceph-mon --id 0 --mkfs --keyring /dev/null
    touch ${MON_DATA}/keyring
    cp ${MON_DATA}/keyring /etc/ceph/keyring
    ceph-mon --id 0

    # start a OSD daemon
    OSD_DATA=${test_dir}/osd
    mkdir ${OSD_DATA}

    cat >> /etc/ceph/ceph.conf <<EOF
[osd.0]
log file = ${test_dir}/log/osd.log
chdir = ""
osd data = ${OSD_DATA}
osd journal = ${OSD_DATA}.journal
osd journal size = 100
osd objectstore =  memstore
osd class load list = *
debug osd = 20
EOF

    OSD_ID=$(ceph osd create)
    ceph osd crush add osd.${OSD_ID} 1 root=default host=localhost
    ceph-osd --id ${OSD_ID} --mkjournal --mkfs
    ceph-osd --id ${OSD_ID}

    # start a MDS daemon
    MDS_DATA=${TEST_DIR}/mds
    mkdir -p $MDS_DATA

    ceph osd pool create cephfs_data 16
    ceph osd pool create cephfs_metadata 16
    ceph fs new cephfs cephfs_metadata cephfs_data

    ceph-mds --id a
    while [[ ! $(ceph mds stat | grep "up:active") ]]; do sleep 1; done

    # start a MGR daemon
    ceph-mgr --id 0

    export CEPH_CONF="/etc/ceph/ceph.conf"

    # copy the CLS libs to the appropriate locations.
    mkdir -p /usr/lib/x86_64-linux-gnu/rados-classes/
    cp debug/libcls_arrow* /usr/lib/x86_64-linux-gnu/rados-classes/
    systemctl restart ceph-osd.target

    # mount a ceph filesystem to /mnt/cephfs in the user-space using ceph-fuse
    fusermount -uz /mnt/cephfs
    rm -rf /mnt/cephfs
    mkdir -p /mnt/cephfs
    ceph-fuse /mnt/cephfs
    sleep 5

    # download an example dataset and copy into the mounted dir
    rm -rf nyc*
    wget https://raw.githubusercontent.com/JayjeetAtGithub/zips/main/nyc.zip # try to get this dataset into the source tree
    unzip nyc.zip
    cp -r nyc /mnt/cephfs/
    sleep 5
    
    ceph -s
    sleep 2

    # run the tests
    TESTS=debug/arrow-cls-cls-arrow-test
    if [ -f "$TESTS" ]; then
       debug/arrow-cls-cls-arrow-test
    fi
popd
