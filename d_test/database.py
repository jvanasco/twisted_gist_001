from twisted.enterprise import adbapi


dbPool = adbapi.ConnectionPool(\
        'sqlite3', 
        'test_database', 
    )


def get_dbPool():
    return dbPool
