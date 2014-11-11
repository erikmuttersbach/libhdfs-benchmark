################################################################################
#    Copyright (C) 2011 HPCC Systems.
#
#    All rights reserved. This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU Affero General Public License as
#    published by the Free Software Foundation, either version 3 of the
#    License, or (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU Affero General Public License for more details.
#
#    You should have received a copy of the GNU Affero General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses/>.
################################################################################


# - Try to find the LIBHDFS xml library
# Once done this will define
#
#  LIBHDFS_FOUND - system has the LIBHDFS library
#  LIBHDFS_INCLUDE_DIR - the LIBHDFS include directory
#  LIBHDFS_LIBRARIES - The libraries needed to use LIBHDFS

if (NOT LIBHADOOP_FOUND)
    include(FindPackageHandleStandardArgs)

    find_library(LIBHADOOP_LIBRARY hadoop PATHS /usr/local/hadoop/lib/native)

    find_package_handle_standard_args(LIBHADOOP DEFAULT_MSG LIBHADOOP_LIBRARY)
endif()
