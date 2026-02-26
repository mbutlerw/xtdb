package xtdb.api.log

import xtdb.database.Database
import xtdb.database.DatabaseName

sealed interface DbOp {
    data class Attach(val dbName: DatabaseName, val config: Database.Config) : DbOp
    data class Detach(val dbName: DatabaseName) : DbOp
}
