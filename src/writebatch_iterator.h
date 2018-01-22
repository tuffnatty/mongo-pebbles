/**
 *    Copyright (C) 2014 MongoDB Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the GNU Affero General Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

 /*   Note: PebblesDB does not contain an iterator over a WriteBatch. This file
  *   contains an implementation for WriteBatch Iterator to iterate through the
  *   records of a WriteBatch. A C++ unordered map is created for mapping each
  *   write of a WriteBatch with the type of the value that it contains (whether
  *   it is a data value or a guard value) as well as if the value has been
  *   deleted before the WriteBatch is committed to storage
  */



#pragma once

#include <atomic>
#include <map>
#include <stack>
#include <string>
#include <vector>
#include <unordered_map>

#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>

#include <pebblesdb/slice.h>
#include <pebblesdb/write_batch.h>


namespace leveldb {

  enum ValueType {
    kTypeDeletion = 0x0,
    kTypeValue = 0x1,
    kTypeGuard = 0x2
  };

  struct WriteBatchRep {
    ValueType type;
    std::string value;

    WriteBatchRep() {
      type = ValueType::kTypeValue;
    }
  };


  class WriteBatchRepHandler {

  public:

    bool Add(const Slice& key, const Slice& value) {

      WriteBatchRep writeBatchRep;
      std::string temp_val = value.ToString();
      auto iter = writeBatchRepMap.find(key.ToString());
      if(iter == writeBatchRepMap.end()) {
	writeBatchRep.type = ValueType::kTypeValue;
	writeBatchRep.value = temp_val;

	std::pair< std::string, WriteBatchRep > recordEntry (key.ToString(), writeBatchRep);
	writeBatchRepMap.insert(recordEntry);

	return true;
      }

      (iter->second).value = temp_val;
      (iter->second).type = ValueType::kTypeValue;

      return true;
    }

    bool Delete(const Slice& key) {

      auto iter = writeBatchRepMap.find(key.ToString());
      if(iter == writeBatchRepMap.end())
	return false;

      (iter->second).type = ValueType::kTypeDeletion;
      return true;
    }

    std::string Find(const Slice& key, bool *is_present) {

      auto iter = writeBatchRepMap.find(key.ToString());

      if(iter == writeBatchRepMap.end()) {
	*is_present = false;
	return std::string();
      }

      WriteBatchRep writeBatchRep;
      writeBatchRep = iter->second;

      if(writeBatchRep.type == ValueType::kTypeDeletion) {
	*is_present = true;
	return std::string();
      }

      *is_present = true;
      return writeBatchRep.value;

    }

    bool Clear() {
      writeBatchRepMap.clear();
      return true;
    }

  private:

    std::unordered_map< std::string, WriteBatchRep > writeBatchRepMap;
  };

}
