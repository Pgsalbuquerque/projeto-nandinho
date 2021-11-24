import pandas as pd
import numpy as np
pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", None)

def generate_parquet3():
    file = open("index.parquet3", "w")
    file.write("asdasdasd")
    file.close()

def convertCsvToParquet(csvPath: str, parquetPath: str):
    csv = pd.read_csv(csvPath)
    csv.to_parquet(parquetPath)

def convertParquetToCsv(parquetPath: str, csvPath: str ):
    csv = pd.read_parquet(parquetPath)
    csv.to_csv(csvPath)

def parquet40Lines(parquetPath: str):
    df = pd.read_parquet(f"{parquetPath}.parquet3", engine="pyarrow")
    path = f"{parquetPath}-40.parquet3"
    df.head(40).to_parquet(path)

def floatToBooleanColumn(parquetPath: str, column: str):
    df = pd.read_parquet(f'{parquetPath}.parquet3', engine="pyarrow")
    for x in range(0, len(df[column])):
        if (df.loc[x, column] == 1.0):
            df.loc[x, column] = True
        else:
            df.loc[x, column] = False
    df.to_parquet(f'{parquetPath}.parquet3')


def readParquet(parquetPath: str):
    df = pd.read_parquet(f'{parquetPath}.parquet3', engine="pyarrow")
    print(df.head(40))

def readParquetColumn(parquetPath: str, column: str):
    df = pd.read_parquet(f'{parquetPath}.parquet3', engine="pyarrow")
    for x in range(0,200):
        # if np.isnan(df[column][x]):
        #     print("si")
        print(type(df[column][x]))

def clearDataframe(parquetPath: str):
    dataframe = pd.read_parquet(f'{parquetPath}.parquet3', engine="pyarrow")
    print(np.finfo(1.0))
    print(np.finfo("1.0"))
    columns = dataframe.columns
    trash = dataframe.to_numpy()

def printAnomalousLine(line: np.array, count: int):
    error = ""
    if line[0] == "" or line[0] == None:
        error += f"ID empty {str(line[0])} | "
    if line[1] == "" or line[1] == None:
        error += f"accountID empty {str(line[1])} | "
    if line[2] == "" or line[2] == None:
        error += f"deviceID empty {str(line[2])} | "
    if line[3] == "" or line[3] == None:
        error += f"installationID empty {str(line[3])} | "
    if line[5] != 1 and line[5] != 0:
        error += f"isFromOfficialStore not bool {str(line[5])} | "
    if line[6] != 1 and line[6] != 0:
        error += f"isEmulator not bool {str(line[6])} | "
    if line[7] != 1 and line[7] != 0:
        error += f"hasFakeLocationApp not bool {str(line[7])} | "
    if line[8] != 1 and line[8] != 0:
        error += f"hasFakeLocationEnabled not bool {str(line[8])} | "
    if line[9] != 1 and line[9] != 0:
        error += f"probableRoot not bool {str(line[9])} | "
    if line[12] != 1 and line[12] != 0:
        error += f"neverPermittedLocationOnAccount not bool {str(line[12])} | "
    if line[16] != 1 and line[16] != 0:
        error += f"ato not bool {str(line[16])} | "
    
    if(error != ""):
        print(error + str(count))
        return True
    return False


def lineIsSafe(line: np.array):
    if line[0] == "" or line[0] == None:
        return False
    elif line[1] == "" or line[1] == None:
        return False
    elif line[2] == "" or line[2] == None:
        return False
    elif line[3] == "" or line[3] == None:
        return False
    elif line[5] != 1 and line[5] != 0:
        return False
    elif line[6] != 1 and line[6] != 0:
        return False
    elif line[7] != 1 and line[7] != 0:
        return False
    elif line[8] != 1 and line[8] != 0:
        return False
    elif line[9] != 1 and line[9] != 0:
        return False
    elif line[12] != 1 and line[12] != 0:
        return False
    elif line[16] != 1 and line[16] != 0:
        return False
    
    return True
    
    

def searchAnomalies(parquetPath: str):
    print("*--abrindo database--*")
    dataframe = pd.read_parquet(f'{parquetPath}.parquet3', engine="pyarrow")
    print("*--convertendo database--*")
    trash = dataframe.to_numpy()
    print("*--iniciando busca--*")
    nRowsErrors = 0
    count = 0
    for row in trash:
        if(printAnomalousLine(row, count)):
            nRowsErrors += 1
        count += 1
    print(f"Numero de linhas: {count}")
    print(f"Numero de linhas falhas: {nRowsErrors}")

def cleannerDatabase(parquetPath: str):
    print("*--abrindo database--*")
    dataframe = pd.read_parquet(f'{parquetPath}.parquet3', engine="pyarrow")
    columns = dataframe.columns
    print("*--convertendo database--*")
    trash = dataframe.to_numpy()
    print("*--percorrendo a database--*")
    count = 0
    indexes = np.zeros(246)
    index = 0
    for row in trash:
        if(lineIsSafe(row) == False):
            indexes[index] = count
            index += 1
        count += 1
    print("*--deletando os itens anomalos--*")
    trash = np.delete(trash, indexes)
    print(len(trash))
    print("*--criando nova database--*")
    database = pd.DataFrame(trash, columns=columns)
    print("*--salvando a database--*")
    database.to_parquet("teste.parquet3")


def deleteAnomaliesLines(parquetPath: str):
    pass

if (__name__ == "__main__"):
    # floatToBooleanColumn("logins-40", "is_emulator")
    cleannerDatabase("logins")
    
        
