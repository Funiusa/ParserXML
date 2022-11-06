import fnmatch
import ftplib
from os import listdir, remove
from zipfile import ZipFile
import xml.etree.ElementTree as ET
import pymysql


def getFilesXML(host, ftp_user, ftp_password):
    ftp = ftplib.FTP(host)
    ftp.login(ftp_user, ftp_password)
    # ftp.retrlines("LIST") # Show all files in directory

    # directory with files
    ftp.cwd('/94fz/Arkhangelskaja_obl/contracts/')  # Test path
    # Get all files
    files = ftp.nlst()

    # Download files
    for file in files:
        if fnmatch.fnmatch(file, '*.xml.zip'):  # To download specific files.
            print("Downloading..." + file)
            try:
                ftp.retrbinary("RETR " + file, open("./XML_Zip/" + file, 'wb').write)  # change dir
            except EOFError:  # To avoid EOF errors.
                pass
    ftp.close()


def unzipFiles():
    path = "./XML_Zip/"
    for file in listdir(path):
        with ZipFile(path + file, 'r') as zObject:
            zObject.extractall("./XML_files/")
        remove(path + file)


def createDict(source):
    retDict = {}
    for elem in source:
        if elem.text is not None and elem.text.isprintable() is False:
            retDict[elem.tag.split('}')[1]] = createDict(elem)
            continue
        retDict[elem.tag.split('}')[1]] = elem.text
    return retDict


def openDict(source):
    for key, val in source.items():
        if type(val) is dict:
            print("-------------")
            print(f'{key}:')
            openDict(val)
        else:
            print(f'{key}: {val}')
            create_table()

def parsFileXML():
    tree = ET.parse('./XML_files/contract__Arkhangelskaja_obl_inc_20110101_000000_20110201_000000_9.xml')
    root = tree.getroot()
    tagsDict = {}
    testDict = {}

    for tags in root[0]:
        if tags.text.isprintable() is not True:
            tagsDict[tags.tag.split('}')[1]] = createDict(tags)
        else:
            tagsDict[tags.tag.split('}')[1]] = tags.text
            testDict[tags.tag.split('}')[1]] = tags.text
    openDict(tagsDict)
    print("=" * 50)
    #openDict(testDict)
    return testDict


def crateTableWithAllItems(connection, tableName, source):
    with connection.cursor() as cursor:
        create_table_query = f"""CREATE TABLE {tableName} (contr_id INT AUTO_INCREMENT, """
        for key in source.keys():
            create_table_query += f" {key} VARCHAR(255),"
        create_table_query = create_table_query[:-1] + ")"
        print(create_table_query)


def create_table(connection, tableName):
    with connection.cursor() as cursor:
        create_table_query = f"""CREATE TABLE { tableName } (
                                    id INT,
                                    regNum LONG,
                                    number INT,
                                    publishDate VARCHAR(20),
                                    signDate DATE,
                                    versionNumber INT,
                                    documentBase VARCHAR(50),
                                    price INT,
                                    currentContractStage VARCHAR(50)
                                    )"""
        print(f"Table \"{tableName}\" successfully create.")
        cursor.execute(create_table_query)


def insert_in_table(connection, tableName, source):
    with connection.cursor() as cursor:
        sql = f"INSERT INTO {tableName} ("
        for key in source.keys():
            sql += f" {key},"
        sql = sql[:-1] + ") VALUES ("

        for val in source.values():
            sql += f" '{val}',"
        sql = sql[:-1] + ");"
        #print(sql)
        cursor.execute(sql)


def delete_from_table(connection, tableName):
    with connection.cursor() as cursor:
        cursor.execute(f"DELETE from { tableName } where NAME='Ram';")


def drop_table(connection, tableName):
    with connection.cursor() as cursor:
        sql = f"""DROP TABLE IF EXISTS { tableName };"""
        cursor.execute(sql)
        print(f"Table \"{ tableName }\" successfully dropped.")


def create_connection():
    try:
        connection = pymysql.connect(
            host="localhost",
            user="root",
            password="380115330",
            port=3306,
            database="my_db",
            cursorclass=pymysql.cursors.DictCursor
        )
        print("Successfully connected!")
        print()
        tableName = "contract"
        try:
            with connection.cursor() as cursor:

                create_table(connection, tableName)
                insert_in_table(connection, tableName, parsFileXML())
                #drop_table(connection, tableName)

                # delete_from_table(connection, tableName)
                connection.commit()
                select_all_raws = f"SELECT * FROM { tableName }"
                cursor.execute(select_all_raws)
                myres = cursor.fetchall()
                for x in myres:
                    print(x)
        finally:
            connection.close()
    except Exception as ex:
        print("Connection refused....")
        print(ex)



if __name__ == "__main__":
    # getFilesXML('ftp.zakupki.gov.ru', 'free', 'free')
    # unzipFiles()
    print()
    # parsFileXML()
    create_connection()

