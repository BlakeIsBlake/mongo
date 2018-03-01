/**
 *    Copyright (C) 2018 MongoDB Inc.
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
 *    must comply with the GNU Affero General Public License in all respects
 *    for all of the code used other than as permitted herein. If you modify
 *    file(s) with this exception, you may extend this exception to your
 *    version of the file(s), but you are not obligated to do so. If you do not
 *    wish to do so, delete this exception statement from your version. If you
 *    delete this exception statement from all source files in the program,
 *    then also delete it in the license file.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kSharding

#include "mongo/db/auth/authorization_session.h"
#include "mongo/db/commands.h"
#include "mongo/db/commands/clone.cpp"
#include "mongo/db/s/config/sharding_catalog_manager.h"
#include "mongo/rpc/get_status_from_command_result.h"
#include "mongo/s/grid.h"
#include "mongo/s/request_types/clone_unsharded_collections_gen.h"

namespace mongo {
namespace {

class CloneUnshardedCollectionsCommand : public BasicCommand {
public:
	CloneUnshardedCollectionsCommand() : BasicCommand("_cloneUnshardedCollections") {}

    std::string help() const override {
        return "should not be calling this directly";
    }

    AllowedOnSecondary secondaryAllowed(ServiceContext*) const override {
        return AllowedOnSecondary::kNever;
    }

    bool adminOnly() const override {
        return true;
    }

    virtual bool supportsWriteConcern(const BSONObj& cmd) const override {
        return true;
    }

    Status checkAuthForCommand(Client* client,
                               const std::string& dbname,
                               const BSONObj& cmdObj) const override {
        if (!AuthorizationSession::get(client)->isAuthorizedForActionsOnResource(
                ResourcePattern::forClusterResource(), ActionType::internal)) {
            return Status(ErrorCodes::Unauthorized, "Unauthorized");
        }
        return Status::OK();
    }

    virtual std::string parseNs(const std::string& dbname, const BSONObj& cmdObj) const {
            const auto nsElt = cmdObj.firstElement();
            uassert(ErrorCodes::InvalidNamespace,
                    "'_cloneUnshardedCollections' must be of type String",
                    nsElt.type() == BSONType::String);
            return nsElt.str();
        }

    bool run(OperationContext* opCtx,
                 const std::string& dbname_unused,
                 const BSONObj& cmdObj,
                 BSONObjBuilder& result) override {
    		if (serverGlobalParams.clusterRole != ClusterRole::ShardServer) {
			return CommandHelpers::appendCommandStatus(
				result,
				Status(ErrorCodes::IllegalOperation,
					   "_cloneUnshardedCollections can only be run on shard servers"));
		}

    		const auto cloneUnshardedCollectionsRequest =
			CloneUnshardedCollections::parse(IDLParserErrorContext("_cloneUnshardedCollections"), cmdObj);
    		const auto dbname = parseNs("", cmdObj);

    		uassert(
			ErrorCodes::InvalidNamespace,
			str::stream() << "invalid db name specified: " << dbname,
			NamespaceString::validDBName(dbname, NamespaceString::DollarInDbNameBehavior::Allow));

		if (dbname == NamespaceString::kAdminDb || dbname == NamespaceString::kConfigDb ||
			dbname == NamespaceString::kLocalDb) {
			return CommandHelpers::appendCommandStatus(
				result,
				{ErrorCodes::InvalidOptions,
				 str::stream()
				 << "Can't clone unsharded collections for " << dbname << " database"});
		}

    		auto from = cloneUnshardedCollectionsRequest.getFrom().toString();

    		if (from.empty()) {
    			return CommandHelpers::appendCommandStatus(
				result,
				{ErrorCodes::InvalidOptions,
				 str::stream() << "Can't run _cloneUnshardedCollections without a source"});
    		}

    		auto const catalogManager = ShardingCatalogManager::get(opCtx);
    		auto const shardRegistry = Grid::get(opCtx)->shardRegistry();

    		const auto shardedColls = catalogManager->getAllShardedCollectionsForDb(opCtx, dbname);

    		BSONArrayBuilder barr;
		for (const auto& shardedColl : shardedColls) {
			barr.append(shardedColl.ns());
		}

    		BSONObj cloneCmdObj;

    		BSONObjBuilder cloneCommandBuilder;
    		cloneCommandBuilder << "clone" << from
    					    << "collsToIgnore" << barr.arr()
						<< bypassDocumentValidationCommandOption() << true;

    		auto cloneResult = CommandHelpers::runCommandDirectly(
			opCtx, OpMsgRequest::fromDBAndBody(dbname, cloneCommandBuilder.obj()));

    		uassertStatusOK(getStatusFromCommandResult(cloneResult));
    		return true;
    }

}  // namespace
}  // namespace mongo

}
