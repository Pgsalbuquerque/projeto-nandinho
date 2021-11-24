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
    if line[0] == "":
        error += f"ID empty {str(line[0])} | "
    if line[1] == "":
        error += f"accountID empty {str(line[1])} | "
    if line[2] == "":
        error += f"deviceID empty {str(line[2])} | "
    if line[3] == "":
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
    

def searchAnomalies(parquetPath: str):
    print("*--abrindo dataframe--*")
    dataframe = pd.read_parquet(f'{parquetPath}.parquet3', engine="pyarrow")
    print("*--convertendo dataframe--*")
    trash = dataframe.to_numpy()
    print("*--iniciando busca--*")
    count = 0
    for row in trash:
        printAnomalousLine(row, count)
        count += 1

def deleteAnomaliesLines(parquetPath: str):
    pass

if (__name__ == "__main__"):
    # floatToBooleanColumn("logins-40", "is_emulator")
    searchAnomalies("logins")
    
        
