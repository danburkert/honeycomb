#ifndef HA_CLOUD_H
#define HA_CLOUD_H

/* Copyright (c) 2004, 2010, Oracle and/or its affiliates. All rights reserved.

  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation; version 2 of the License.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with this program; if not, write to the Free Software
  Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

/** @file ha_cloud.h

    @brief
  The ha_cloud engine is a cloudbed storage engine for cloud purposes only;
  it does nothing at this point. Its purpose is to provide a source
  code illustration of how to begin writing new storage engines; see also
  /storage/cloud/ha_cloud.cc.

    @note
  Please read ha_cloud.cc before reading this file.
  Reminder: The cloud storage engine implements all methods that are *required*
  to be implemented. For a full list of all methods that you can implement, see
  handler.h.

   @see
  /sql/handler.h and /storage/cloud/ha_cloud.cc
*/

#ifdef USE_PRAGMA_INTERFACE
#pragma interface			/* gcc class implementation */
#endif

#include "CloudHandler.h"
#include "CloudShare.h"
#include "Util.h"
#include "Macros.h"
#include "JNISetup.h"
#include "Logging.h"

#endif
