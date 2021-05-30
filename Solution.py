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
    return Query(result[0]['queryID'], result[0]['purpose'], result[0]['size'])

def buildDisk(result: ResultSet, rows_effected: int) -> Disk:
    if rows_effected != 1:
        raise Execption()
    return Disk(result[0]['diskID'], result[0]['company'], result[0]['speed'], result[0]['free_space'], result[0]['cost'])

def buildRAM(result: ResultSet, rows_effected: int) -> RAM:
    if rows_effected != 1:
        raise Execption()
    return RAM(result[0]['ramID'], result[0]['company'], result[0]['size'])

def buildAvailableDiskList(result: ResultSet, rows_effected: int) -> List[int]:
    ret = []
    for i in range(0, rows_effected):
        ret.append(result[i]['diskID'])
    return ret

def createTables():
    conn = None
    try:
        conn = Connector.DBConnector()
        transaction = sql.SQL(
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
            "); "
            
            "CREATE VIEW QueriesDisksWithSizes AS "
            "SELECT q.queryID, d.diskID, size, free_space "
            "FROM Queries q " 
            "INNER JOIN QueriesDisks qd "
            "ON q.queryID = qd.queryID "
            "INNER JOIN Disks d "
            "ON qd.diskID = d.diskID; "
            
            "CREATE VIEW PossibleQueriesDisks AS "
            "SELECT q.queryID, d.diskID, d.speed "
            "FROM Queries q, Disks d "
            "WHERE q.size <= d.free_space ")
        conn.execute(transaction)
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
        transaction = sql.SQL("DELETE FROM Queries; "
                        "DELETE FROM Disks; "
                        "DELETE FROM RAMs;")
        conn.execute(transaction)
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
        transaction = sql.SQL("DROP TABLE IF EXISTS Queries CASCADE; "
                        "DROP TABLE IF EXISTS Disks CASCADE; "
                        "DROP TABLE IF EXISTS RAMs CASCADE; "
                        "DROP TABLE IF EXISTS QueriesDisks CASCADE; "
                        "DROP TABLE IF EXISTS RAMsDisks CASCADE; ")
        conn.execute(transaction)
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
        transaction = sql.SQL("INSERT INTO Queries VALUES ("
                        "{queryID}, "
                        "{purpose}, "
                        "{size})").format(queryID=sql.Literal(query.getQueryID()), \
                                          purpose=sql.Literal(query.getPurpose()), \
                                          size=sql.Literal(query.getSize()))
        conn.execute(transaction)
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
        transaction = sql.SQL("SELECT * FROM Queries WHERE queryID = {ID}".format(ID=queryID))
        rows_effected, result = conn.execute(transaction)
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
        transaction = sql.SQL("UPDATE Disks d "
                              "SET free_space = qdws.free_space + qdws.size "
                              "FROM QueriesDisksWithSizes qdws "
                              "WHERE qdws.queryID = {ID} AND qdws.diskID = d.diskID; "
                              "DELETE FROM Queries WHERE queryID = {ID}")\
                              .format(ID=sql.Literal(query.getQueryID()))
        conn.execute(transaction)
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        ret = ReturnValue.ERROR
        conn.rollback()
    except Exception as e:
        print(e)
        ret = ReturnValue.ERROR
        conn.rollback()
    finally:
        conn.close()
        return ret


def addDisk(disk: Disk) -> ReturnValue:
    conn = None
    ret = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        transaction = sql.SQL("INSERT INTO Disks VALUES ("
                        "{diskID}, "
                        "{company}, "
                        "{speed}, "
                        "{free_space}, "
                        "{cost})").format(diskID=sql.Literal(disk.getDiskID()), \
                                          company=sql.Literal(disk.getCompany()), \
                                          speed=sql.Literal(disk.getSpeed()), \
                                          free_space=sql.Literal(disk.getFreeSpace()), \
                                          cost=sql.Literal(disk.getCost()))
        conn.execute(transaction)
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


def getDiskProfile(diskID: int) -> Disk:
    conn = None
    rows_effected, result = 0, ResultSet()
    try:
        conn = Connector.DBConnector()
        transaction = sql.SQL("SELECT * FROM Disks WHERE diskID = {ID}".format(ID=diskID))
        rows_effected, result = conn.execute(transaction)
        ret = buildDisk(result, rows_effected)
        conn.commit()
    except Exception as e:
        ret = Disk.badDisk()
    finally:
        conn.close()
        return ret


def deleteDisk(diskID: int) -> ReturnValue:
    conn = None
    ret = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        transaction = sql.SQL("DELETE FROM Disks WHERE diskID = {ID}").format(ID=sql.Literal(diskID))
        conn.execute(transaction)
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        ret = ReturnValue.ERROR
    except Exception as e:
        ret = ReturnValue.ERROR
    finally:
        conn.close()
        return ret


def addRAM(ram: RAM) -> ReturnValue:
    conn = None
    ret = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        transaction = sql.SQL("INSERT INTO RAMs VALUES ("
                        "{ramID}, "
                        "{size}, "
                        "{company})").format(ramID=sql.Literal(ram.getRamID()), \
                                          size=sql.Literal(ram.getSize()), \
                                          company=sql.Literal(ram.getCompany()))
        conn.execute(transaction)
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


def getRAMProfile(ramID: int) -> RAM:
    conn = None
    rows_effected, result = 0, ResultSet()
    try:
        conn = Connector.DBConnector()
        transaction = sql.SQL("SELECT * FROM RAMs WHERE ramID = {ID}".format(ID=ramID))
        rows_effected, result = conn.execute(transaction)
        ret = buildRAM(result, rows_effected)
        conn.commit()
    except Exception as e:
        ret = RAM.badRAM()
    finally:
        conn.close()
        return ret

def deleteRAM(ramID: int) -> ReturnValue:
    conn = None
    ret = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        transaction = sql.SQL("DELETE FROM RAMs WHERE ramID = {ID}").format(ID=sql.Literal(ramID))
        conn.execute(transaction)
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        ret = ReturnValue.ERROR
    except Exception as e:
        ret = ReturnValue.ERROR
    finally:
        conn.close()
        return ret


def addDiskAndQuery(disk: Disk, query: Query) -> ReturnValue:
    conn = None
    ret = ReturnValue.OK
    try:
        conn = Connector.DBConnector()
        transaction = sql.SQL("INSERT INTO Disks VALUES ("
                        "{diskID}, "
                        "{company}, "
                        "{speed}, "
                        "{free_space}, "
                        "{cost});"
                        
                        "INSERT INTO Queries VALUES ("
                        "{queryID}, "
                        "{purpose}, "
                        "{size})").format(queryID=sql.Literal(query.getQueryID()), \
                                          purpose=sql.Literal(query.getPurpose()), \
                                          size=sql.Literal(query.getSize()), \
                                          diskID=sql.Literal(disk.getDiskID()), \
                                          company=sql.Literal(disk.getCompany()), \
                                          speed=sql.Literal(disk.getSpeed()), \
                                          free_space=sql.Literal(disk.getFreeSpace()), \
                                          cost=sql.Literal(disk.getCost()))
        conn.execute(transaction)
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        ret = ReturnValue.ERROR
        conn.rollback()
    except DatabaseException.UNIQUE_VIOLATION as e:
        ret = ReturnValue.ALREADY_EXISTS
        conn.rollback()
    except Exception as e:
        ret = ReturnValue.ERROR
        conn.rollback()
    finally:
        conn.close()
        return ret


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
    conn = None
    ret = []
    try:
        conn = Connector.DBConnector()
        transaction = sql.SQL("SELECT diskID, COUNT(queryID), speed "
                              "FROM PossibleQueriesDisks "
                              "GROUP BY diskID, speed "
                              "ORDER BY COUNT(queryID) DESC, speed DESC, diskID ASC "
                              "LIMIT 5")
        rows_effected, result = conn.execute(transaction)
        ret = buildAvailableDiskList(result, rows_effected)
        conn.commit()
    except Exception as e:
        print(e)
        ret = []
    finally:
        conn.close()
        return ret


def getCloseQueries(queryID: int) -> List[int]:
    return [];    


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
    if addDisk(Disk(1, "supreme", 200, 1000, 1)) != ReturnValue.OK:
        print("add good disk error")
    if addDisk(Disk(2, "supreme", 0, 1000, 1)) != ReturnValue.BAD_PARAMS:
        print("add bad disk error")
    if addDisk(Disk(1, "supreme", 200, 1000, 1)) != ReturnValue.ALREADY_EXISTS:
        print("duplicate disk error")
    if getDiskProfile(1).getCompany() != "supreme":
        print("get good disk error")
    if getDiskProfile(2).getCompany() != None:
        print("get non existent disk error")
    if addDisk(Disk(2, "supreme", 200, 1000, 1)) != ReturnValue.OK:
        print("add good disk error")
    if deleteDisk(2) != ReturnValue.OK:
        print("delete disk error")
    if deleteDisk(15) != ReturnValue.OK:
        print("delete disk error")
    if addRAM(RAM(1, "gucci", 4000)) != ReturnValue.OK:
        print("add good RAM error")
    if addRAM(RAM(2, "gucci", -1)) != ReturnValue.BAD_PARAMS:
        print("add bad RAM error")
    if addRAM(RAM(1, "gucci", 4000)) != ReturnValue.ALREADY_EXISTS:
        print("duplicate RAM error")
    if getRAMProfile(1).getCompany() != "gucci":
        print("get good RAM error")
    if getRAMProfile(2).getCompany() != None:
        print("get nonexistent RAM error")
    if addRAM(RAM(2, "gucci", 4000)) != ReturnValue.OK:
        print("add good RAM error")
    if deleteRAM(2) != ReturnValue.OK:
        print("delete RAM error")
    if deleteRAM(15) != ReturnValue.OK:
        print("delete RAM error")
    if addDiskAndQuery(Disk(2, "supreme", 400, 1000, 1), Query(2, "something else", 5)) != ReturnValue.OK:
        print("add good disk and query error")
    if getDiskProfile(2).getCompany() != "supreme":
        print("add good disk and query error")
    if getQueryProfile(2).getPurpose() != "something else":
        print("add good disk and query error")
    if addDiskAndQuery(Disk(3, "supreme", 200, 1000, 1), Query(2, "something else", 5)) != ReturnValue.ALREADY_EXISTS:
        print("add duplicate query with disk error")
    if getDiskProfile(3).getCompany() != None:
        print("added disk with duplicate query error")
    if addDiskAndQuery(Disk(2, "supreme", 200, 1000, 1), Query(3, "something else", 5)) != ReturnValue.ALREADY_EXISTS:
        print("add duplicate disk with query error")
    if getQueryProfile(3).getPurpose() != None:
        print("added query with duplicate disk error")
    if addDiskAndQuery(Disk(3, "supreme", 200, 2000, 1), Query(3, "something else", 2000)) != ReturnValue.OK:
        print("add good disk and query error")
    if addDisk(Disk(4, "supreme", 200, 1000, 1)) != ReturnValue.OK:
        print("add good disk error")
    if addDisk(Disk(6, "supreme", 200, 1000, 1)) != ReturnValue.OK:
        print("add good disk error")
    if addDisk(Disk(5, "supreme", 200, 1000, 1)) != ReturnValue.OK:
        print("add good disk error")
    if addDisk(Disk(7, "supreme", 200, 1000, 1)) != ReturnValue.OK:
        print("add good disk error")
    print(mostAvailableDisks())
    print("Clearing all tables")
    clearTables()
    print(mostAvailableDisks())
    if getQueryProfile(1).getQueryID() != None:
        print("clear tables error")
    if getDiskProfile(1).getDiskID() != None:
        print("clear tables error")
    if getRAMProfile(1).getRamID() != None:
        print("clear tables error")
    print("Dropping all tables - empty database")
    dropTables()