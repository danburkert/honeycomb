# Copyright (c) 2006, 2010, Oracle and/or its affiliates. All rights reserved.
# 
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; version 2 of the License.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301 USA

include_directories("/usr/local/include/boost-1_50_0/")
include_directories("/usr/local/include/thrift")

SET(cloud_PLUGIN_DYNAMIC "ha_cloud")
SET(cloud_SOURCES CloudHandler.cc ha_cloud.cc gen-cpp/hbase_engine_types.cpp gen-cpp/Engine.cpp gen-cpp/hbase_engine_constants.cpp)
MYSQL_ADD_PLUGIN(cloud ${cloud_SOURCES} STORAGE_ENGINE MODULE_ONLY)

SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fexceptions")
SET_TARGET_PROPERTIES(cloud PROPERTIES LINK_FLAGS -lthrift)
