from typing import List
import Utility.DBConnector as Connector
from Utility.ReturnValue import ReturnValue
from Utility.Exceptions import DatabaseException
from Utility.DBConnector import ResultSet
from Business.Query import Query
from Business.RAM import RAM
from Business.Disk import Disk
from psycopg2 import sql

def buildQuery(result: ResultSet, rows_effected: int) -> Query:
    if rows_effected != 1:
        raise Execption()
    return Query(result[0]['queryId'], result[0]['purpose'], result[0]['size'])

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
        conn.close()


def addQuery(query: Query) -> ReturnValue:
    conn = None
    ret = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO Queries VALUES ("
                        "{queryID}, "
                        "{purpose}, "
                        "{size})").format(queryID=sql.Literal(query.getQueryID()), \
                                          purpose=sql.Literal(query.getPurpose()), \
                                          size=sql.Literal(query.getSize()))
        conn.execute(query)
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        ret = ReturnValue.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
        ret = ReturnValue.BAD_PARAMS
    except DatabaseException.CHECK_VIOLATION as e:
        ret = ReturnValue.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
        ret = ReturnValue.ALREADY_EXISTS
    except Exception as e:
        ret = ReturnValue.ERROR
    finally:
        conn.close()
        return ret

def getQueryProfile(queryID: int) -> Query:
    conn = None
    rows_effected, result = 0, ResultSet()
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("SELECT * FROM Queries WHERE queryID = {ID}".format(ID=queryID))
        rows_effected, result = conn.execute(query)
        ret = buildQuery(result, rows_effected)
        conn.commit()
    except Exception as e:
        ret = Query.badQuery()
    finally:
        conn.close()
        return ret


def deleteQuery(query: Query) -> ReturnValue:
    conn = None
    ret = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM Queries WHERE queryID = {ID}").format(ID=sql.Literal(query.getQueryID()))
        conn.execute(query)
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        ret = ReturnValue.ERROR
    except Exception as e:
        print(e)
        ret = ReturnValue.ERROR
    finally:
        conn.close()
        return ret


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
    if addQuery(Query(-1, "something", 2)) != ReturnValue.BAD_PARAMS:
        print("Negative id error")
    if addQuery(Query(1, "something", -2)) != ReturnValue.BAD_PARAMS:
        print("Negative size error")
    if addQuery(Query(1, None, 2)) != ReturnValue.BAD_PARAMS:
        print("null values error")
    if addQuery(Query(1, "something", 2)) != ReturnValue.OK:
        print("add good query error")
    if addQuery(Query(1, "something else", 5)) != ReturnValue.ALREADY_EXISTS:
        print("duplicate query error")
    if getQueryProfile(1).getPurpose() != "something":
        print("get good query error")
    if getQueryProfile(0).getQueryID() != None:
        print("get non existent query error")
    if deleteQuery(Query(15, "something else", 5)) != ReturnValue.OK:
        print("delete query error")
    if addQuery(Query(2, "something else", 5)) != ReturnValue.OK:
        print("add good query error")
    if getQueryProfile(2).getPurpose() != "something else":
        print("get good query error")
    if deleteQuery(Query(2, "something else", 5)) != ReturnValue.OK:
        print("delete query error")
    if getQueryProfile(2).getQueryID() != None:
        print("get non existent query error")
    print("Clearing all tables")
    clearTables()
    if getQueryProfile(1).getQueryID() != None:
        print("clear tables error")
    print("Dropping all tables - empty database")
    dropTables()