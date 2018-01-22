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

#include "mongo/base/disallow_copying.h"
#include "mongo/db/server_parameters.h"

#include "pebbles_engine.h"

namespace mongo {

    // To dynamically configure LevelDB's rate limit, run
    // db.adminCommand({setParameter:1, leveldbRuntimeConfigMaxWriteMBPerSec:30})
    class LevelRateLimiterServerParameter : public ServerParameter {
        MONGO_DISALLOW_COPYING(LevelRateLimiterServerParameter);

    public:
        LevelRateLimiterServerParameter(LevelEngine* engine);
        virtual void append(OperationContext* txn, BSONObjBuilder& b, const std::string& name);
        virtual Status set(const BSONElement& newValueElement);
        virtual Status setFromString(const std::string& str);

    private:
        Status _set(int newNum);
        LevelEngine* _engine;
    };

    // We use mongo's setParameter() API to issue a backup request to leveldb.
    // To backup entire LevelDB instance, call:
    // db.adminCommand({setParameter:1, leveldbBackup: "/var/lib/mongodb/backup/1"})
    // The directory needs to be an absolute path. It should not exist -- it will be created
    // automatically.
    class LevelBackupServerParameter : public ServerParameter {
        MONGO_DISALLOW_COPYING(LevelBackupServerParameter);

    public:
        LevelBackupServerParameter(LevelEngine* engine);
        virtual void append(OperationContext* txn, BSONObjBuilder& b, const std::string& name);
        virtual Status set(const BSONElement& newValueElement);
        virtual Status setFromString(const std::string& str);

    private:
        LevelEngine* _engine;
    };

    // We use mongo's setParameter() API to issue a compact request to leveldb.
    // To compact entire LevelDB instance, call:
    // db.adminCommand({setParameter:1, leveldbCompact: 1})
    class LevelCompactServerParameter : public ServerParameter {
        MONGO_DISALLOW_COPYING(LevelCompactServerParameter);

    public:
        LevelCompactServerParameter(LevelEngine* engine);
        virtual void append(OperationContext* txn, BSONObjBuilder& b, const std::string& name);
        virtual Status set(const BSONElement& newValueElement);
        virtual Status setFromString(const std::string& str);

    private:
        LevelEngine* _engine;
    };

    // We use mongo's setParameter() API to dynamically change the size of the block cache
    // To compact entire LevelDB instance, call:
    // db.adminCommand({setParameter:1, leveldbRuntimeConfigCacheSizeGB: 10})
    class LevelCacheSizeParameter : public ServerParameter {
        MONGO_DISALLOW_COPYING(LevelCacheSizeParameter);

    public:
        LevelCacheSizeParameter(LevelEngine* engine);
        virtual void append(OperationContext* txn, BSONObjBuilder& b, const std::string& name);
        virtual Status set(const BSONElement& newValueElement);
        virtual Status setFromString(const std::string& str);

    private:
        Status _set(int newNum);
        LevelEngine* _engine;
    };
    
    
    // We use mongo's setParameter() API to dynamically change the LevelDB options using the SetOptions API
    // To dynamically change an option, call:
    // db.adminCommand({setParameter:1, "leveldbOptions": "someoption=1; someoption2=3"})
    class LevelOptionsParameter : public ServerParameter {
        MONGO_DISALLOW_COPYING(LevelOptionsParameter);

    public:
        LevelOptionsParameter(LevelEngine* engine);
        virtual void append(OperationContext* txn, BSONObjBuilder& b, const std::string& name);
        virtual Status set(const BSONElement& newValueElement);
        virtual Status setFromString(const std::string& str);

    private:
        LevelEngine* _engine;
    };
}
