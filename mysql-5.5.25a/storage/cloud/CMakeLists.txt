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

include_directories("/System/Library/Frameworks/JavaVM.framework/Versions/Current/Headers")

# Finding and including Java still needs to be streamlined, this isn't very portable at the moment. - ABC

include_directories("/usr/lib/jvm/default-java/include")
include_directories("/usr/lib/jvm/default-java/include/linux")

link_directories("/usr/lib/jvm/default-java/jre/lib/amd64")
link_directories("/usr/lib/jvm/default-java/jre/lib/amd64/server")

SET(cloud_PLUGIN_DYNAMIC "ha_cloud")
SET(cloud_SOURCES CloudHandler.cc ha_cloud.cc CloudHandler.h ha_cloud.h HBaseAdapter.cpp)

IF(UNIX)
    MYSQL_ADD_PLUGIN(cloud ${cloud_SOURCES} STORAGE_ENGINE MODULE_ONLY LINK_LIBRARIES java jvm)
ENDIF()

IF(APPLE)
    MYSQL_ADD_PLUGIN(cloud ${cloud_SOURCES} STORAGE_ENGINE MODULE_ONLY)
    SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -framework JavaVM")
ENDIF()

