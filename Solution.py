from typing import List
import Utility.DBConnector as Connector
from Utility.ReturnValue import ReturnValue
from Utility.Exceptions import DatabaseException
from Business.Query import Query
from Business.RAM import RAM
from Business.Disk import Disk
from psycopg2 import sql


def createTables():
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL(
            "CREATE TABLE Queries ("
            "queryID INTEGER, "
            "purpose TEXT NOT NULL, "
            "size INTEGER NOT NULL, "
            "PRIMARY KEY (queryID), "
            "CHECK (queryID > 0), "
            "CHECK (size >= 0)" 
            "); "
            
            "CREATE TABLE Disks ("
            "diskID INTEGER, "
            "company TEXT NOT NULL, "
            "speed INTEGER NOT NULL, "
            "free_space INTEGER NOT NULL, "
            "cost INTEGER NOT NULL, "
            "PRIMARY KEY (diskID), "
            "CHECK (diskID > 0), "
            "CHECK (speed > 0), "
            "CHECK (cost > 0), "
            "CHECK (free_space >= 0)" 
            "); "
            
            "CREATE TABLE RAMs ("
            "ramID INTEGER, "
            "size INTEGER NOT NULL, "
            "company TEXT NOT NULL, "
            "PRIMARY KEY (ramID), "
            "CHECK (ramID > 0), "
            "CHECK (size > 0)" 
            "); "
            
            "CREATE TABLE QueriesDisks ("
            "queryID INTEGER, "
            "diskID INTEGER, "
            "FOREIGN KEY (queryID) "
            "REFERENCES Queries(queryID) "
            "ON DELETE CASCADE, "
            "FOREIGN KEY (diskID) "
            "REFERENCES Disks(diskID) "
            "ON DELETE CASCADE " 
            "); "
            
            "CREATE TABLE RAMsDisks ("
            "ramID INTEGER, "
            "diskID INTEGER, "
            "FOREIGN KEY (ramID) "
            "REFERENCES RAMs(ramID) "
            "ON DELETE CASCADE, "
            "FOREIGN KEY (diskID) "
            "REFERENCES Disks(diskID) "
            "ON DELETE CASCADE " 
            ");")
        conn.execute(query)
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        print(e)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        # will happen any way after try termination or exception handling
        conn.close()


def clearTables():
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM Queries; "
                        "DELETE FROM Disks; "
                        "DELETE FROM RAMs;")
        conn.execute(query)
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        # do stuff
        print(e)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        # do stuff
        print(e)
    except DatabaseException.CHECK_VIOLATION as e:
        # do stuff
        print(e)
    except DatabaseException.UNIQUE_VIOLATION as e:
        # do stuff
        print(e)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        # do stuff
        print(e)
    except Exception as e:
        print(e)
    finally:
        # will happen any way after code try termination or exception handling
        conn.close()


def dropTables():
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DROP TABLE IF EXISTS Queries CASCADE; "
                        "DROP TABLE IF EXISTS Disks CASCADE; "
                        "DROP TABLE IF EXISTS RAMs CASCADE; "
                        "DROP TABLE IF EXISTS QueriesDisks CASCADE; "
                        "DROP TABLE IF EXISTS RAMsDisks CASCADE; ")
        conn.execute(query)
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        # do stuff
        print(e)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        # do stuff
        print(e)
    except DatabaseException.CHECK_VIOLATION as e:
        # do stuff
        print(e)
    except DatabaseException.UNIQUE_VIOLATION as e:
        # do stuff
        print(e)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        # do stuff
        print(e)
    except Exception as e:
        print(e)
    finally:
        # will happen any way after code try termination or exception handling
        conn.close()


def addQuery(query: Query) -> ReturnValue:
    return ReturnValue.OK


def getQueryProfile(queryID: int) -> Query:
    return Query()


def deleteQuery(query: Query) -> ReturnValue:
    return ReturnValue.OK


def addDisk(disk: Disk) -> ReturnValue:
    return ReturnValue.OK


def getDiskProfile(diskID: int) -> Disk:
    return Disk()


def deleteDisk(diskID: int) -> ReturnValue:
    return ReturnValue.OK


def addRAM(ram: RAM) -> ReturnValue:
    return ReturnValue.OK


def getRAMProfile(ramID: int) -> RAM:
    return RAM()


def deleteRAM(ramID: int) -> ReturnValue:
    return ReturnValue.OK


def addDiskAndQuery(disk: Disk, query: Query) -> ReturnValue:
    return ReturnValue.OK


def addQueryToDisk(query: Query, diskID: int) -> ReturnValue:
    return ReturnValue.OK


def removeQueryFromDisk(query: Query, diskID: int) -> ReturnValue:
    return ReturnValue.OK


def addRAMToDisk(ramID: int, diskID: int) -> ReturnValue:
    return ReturnValue.OK


def removeRAMFromDisk(ramID: int, diskID: int) -> ReturnValue:
    return ReturnValue.OK


def averageSizeQueriesOnDisk(diskID: int) -> float:
    return 0


def diskTotalRAM(diskID: int) -> int:
    return 0


def getCostForPurpose(purpose: str) -> int:
    return 0


def getQueriesCanBeAddedToDisk(diskID: int) -> List[int]:
    return []


def getQueriesCanBeAddedToDiskAndRAM(diskID: int) -> List[int]:
    return []


def isCompanyExclusive(diskID: int) -> bool:
    return True


def getConflictingDisks() -> List[int]:
    return []


def mostAvailableDisks() -> List[int]:
    return []


def getCloseQueries(queryID: int) -> List[int]:
    return []


if __name__ == '__main__':
    print("Creating all tables")
    createTables()
    print("Clearing all tables")
    clearTables()
    print("Dropping all tables - empty database")
    dropTables()