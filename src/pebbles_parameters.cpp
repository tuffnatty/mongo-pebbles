/**
*    Copyright (C) 2014 MongoDB Inc.
*
*    This program is free software: you can redistribute it and/or  modify
*    it under the terms of the GNU Affero General Public License, version 3,
*    as published by the Free Software Foundation.
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
#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#include "mongo/platform/basic.h"

#include "pebbles_parameters.h"
#include "pebbles_util.h"

#include "mongo/logger/parse_log_component_settings.h"
#include "mongo/util/log.h"
#include "mongo/util/mongoutils/str.h"

#include <pebblesdb/status.h>
#include <pebblesdb/cache.h>
#include <pebblesdb/db.h>
#include <pebblesdb/options.h>

namespace mongo {

    LevelRateLimiterServerParameter::LevelRateLimiterServerParameter(LevelEngine* engine)
        : ServerParameter(ServerParameterSet::getGlobal(), "leveldbRuntimeConfigMaxWriteMBPerSec",
                          false, true),
          _engine(engine) {}

    void LevelRateLimiterServerParameter::append(OperationContext* txn, BSONObjBuilder& b,
                                                 const std::string& name) {
        b.append(name, _engine->getMaxWriteMBPerSec());
    }

    Status LevelRateLimiterServerParameter::set(const BSONElement& newValueElement) {
        if (!newValueElement.isNumber()) {
            return Status(ErrorCodes::BadValue, str::stream() << name() << " has to be a number");
        }
        return _set(newValueElement.numberInt());
    }

    Status LevelRateLimiterServerParameter::setFromString(const std::string& str) {
        int num = 0;
        Status status = parseNumberFromString(str, &num);
        if (!status.isOK()) return status;
        return _set(num);
    }

    Status LevelRateLimiterServerParameter::_set(int newNum) {
        if (newNum <= 0) {
            return Status(ErrorCodes::BadValue, str::stream() << name() << " has to be > 0");
        }
        log() << "LevelDB: changing rate limiter to " << newNum << "MB/s";
        _engine->setMaxWriteMBPerSec(newNum);

        return Status::OK();
    }

    LevelBackupServerParameter::LevelBackupServerParameter(LevelEngine* engine)
        : ServerParameter(ServerParameterSet::getGlobal(), "leveldbBackup", false, true),
          _engine(engine) {}

    void LevelBackupServerParameter::append(OperationContext* txn, BSONObjBuilder& b,
                                            const std::string& name) {
        b.append(name, "");
    }

    Status LevelBackupServerParameter::set(const BSONElement& newValueElement) {
        auto str = newValueElement.str();
        if (str.size() == 0) {
            return Status(ErrorCodes::BadValue, str::stream() << name() << " has to be a string");
        }
        return setFromString(str);
    }

    Status LevelBackupServerParameter::setFromString(const std::string& str) {
      return Status::OK();
    }

    LevelCompactServerParameter::LevelCompactServerParameter(LevelEngine* engine)
        : ServerParameter(ServerParameterSet::getGlobal(), "leveldbCompact", false, true),
          _engine(engine) {}

    void LevelCompactServerParameter::append(OperationContext* txn, BSONObjBuilder& b,
                                             const std::string& name) {
        b.append(name, "");
    }

    Status LevelCompactServerParameter::set(const BSONElement& newValueElement) {
        return setFromString("");
    }

  
  Status LevelCompactServerParameter::setFromString(const std::string& str) {
    return Status::OK();
  }
  
    LevelCacheSizeParameter::LevelCacheSizeParameter(LevelEngine* engine)
        : ServerParameter(ServerParameterSet::getGlobal(), "leveldbRuntimeConfigCacheSizeGB", false,
                          true),
          _engine(engine) {}

  void LevelCacheSizeParameter::append(OperationContext* txn, BSONObjBuilder& b,
				       const std::string& name) {
  }
  
  Status LevelCacheSizeParameter::set(const BSONElement& newValueElement) {
    if (!newValueElement.isNumber()) {
      return Status(ErrorCodes::BadValue, str::stream() << name() << " has to be a number");
    }
    return _set(newValueElement.numberInt());
  }

    Status LevelCacheSizeParameter::setFromString(const std::string& str) {
        int num = 0;
        Status status = parseNumberFromString(str, &num);
        if (!status.isOK()) return status;
        return _set(num);
    }

  Status LevelCacheSizeParameter::_set(int newNum) {
    if (newNum <= 0) {
      return Status(ErrorCodes::BadValue, str::stream() << name() << " has to be > 0");
    }
    log() << "LevelDB: changing block cache size to " << newNum << "GB";
    return Status::OK();
  }    
  
    LevelOptionsParameter::LevelOptionsParameter(LevelEngine* engine)
        : ServerParameter(ServerParameterSet::getGlobal(), "leveldbOptions", false,
                          true),
          _engine(engine) {}

    void LevelOptionsParameter::append(OperationContext* txn, BSONObjBuilder& b,
                                         const std::string& name) {
    }

    Status LevelOptionsParameter::set(const BSONElement& newValueElement) {        
        // In case the BSON element is not a string, the conversion will fail, 
        // raising an exception catched by the outer layer.
        // Which will generate an error message that looks like this:
        // wrong type for field (leveldbOptions) 3 != 2        
        return setFromString(newValueElement.String());
    }

    Status LevelOptionsParameter::setFromString(const std::string& str) {
        log() << "LevelDB: Attempting to apply settings: " << str;	
        return Status::OK();
    }
}
