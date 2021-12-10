import pandas as pd
import numpy as np
import datetime

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

def printColumns(parquetPath: str):
    dataframe = pd.read_parquet(f'{parquetPath}.parquet3', engine="pyarrow")
    print(dataframe.columns)
    numpydataframe = dataframe.to_numpy()
    counts = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]
    for column in numpydataframe:
        # if(np.isnan(column[0])):
        #     counts[0] = counts[0] + 1
        # if(np.isnan(column[1])):
        #     counts[1] = counts[1] + 1
        # if(np.isnan(column[2])):
        #     counts[2] = counts[2] + 1
        # if(np.isnan(column[3])):
        #     counts[3] = counts[3] + 1
        if(np.isnan(column[4])):
            counts[4] = counts[4] + 1
        if(np.isnan(column[5])):
            counts[5] = counts[5] + 1
        if(np.isnan(column[6])):
            counts[6] = counts[6] + 1
        if(np.isnan(column[7])):
            counts[7] = counts[7] + 1
        if(np.isnan(column[8])):
            counts[8] = counts[8] + 1
        if(np.isnan(column[9])):
            counts[9] = counts[9] + 1
        if(np.isnan(column[10])):
            counts[10] = counts[10] + 1
        if(np.isnan(column[11])):
            counts[11] = counts[11] + 1
        if(np.isnan(column[12])):
            counts[12] = counts[12] + 1
        if(np.isnan(column[13])):
            counts[13] = counts[13] + 1
        if(np.isnan(column[14])):
            counts[14] = counts[14] + 1
        if(np.isnan(column[15])):
            counts[15] = counts[15] + 1
        if(np.isnan(column[16])):
            counts[16] = counts[16] + 1
    print(counts)

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
    elif line[4] == "" or line[4] == None or np.isnan(line[4]):
        return False
    elif line[5] != 1 and line[5] != 0 or np.isnan(line[5]):
        return False
    elif line[6] != 1 and line[6] != 0 or np.isnan(line[6]):
        return False
    elif line[7] != 1 and line[7] != 0 or np.isnan(line[7]):
        return False
    elif line[8] != 1 and line[8] != 0 or np.isnan(line[8]):
        return False
    elif line[9] != 1 and line[9] != 0 or np.isnan(line[9]):
        return False
    elif np.isnan(line[10]):
        return False
    elif np.isnan(line[11]):
        return False
    elif line[12] != 1 and line[12] != 0 or np.isnan(line[12]):
        return False
    elif line[16] != 1 and line[16] != 0 or np.isnan(line[16]):
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
    indexes = []
    for row in trash:
        if row[13] != 1 and row[13] != 0 or np.isnan(row[13]):
            row[13] = 0
        if row[14] != 1 and row[14] != 0 or np.isnan(row[14]):
            row[14] = 0
        if row[15] != 1 and row[15] != 0 or np.isnan(row[15]):
            row[15] = 0
        if(lineIsSafe(row) == False):
            indexes.append(count)
            count += 1
    print("*--deletando os itens anomalos--*")
    trash = np.delete(trash, indexes, axis=0)
    print(f"*-- foram deletadas {len(indexes)} linhas --*")
    print("*--criando nova database--*")
    database = pd.DataFrame(trash, columns=columns)
    print("*--salvando a database--*")
    database.to_parquet("final.parquet3")

def createDateColumn(parquetPath: str):
    print("*--abrindo database--*")
    dataframe = pd.read_parquet(f'{parquetPath}.parquet3', engine="pyarrow")
    print("*--convertendo timestamp--*")
    lista = []
    dataframenumpy = dataframe.to_numpy()
    count = 0
    for linha in dataframenumpy:
        lista.append(datetime.datetime.fromtimestamp(linha[4]/1000.0))
    # datecolumn = pd.to_datetime(dataframe['timestamp'], unit="ms")
    dataframe["local_date_time"] = lista
    print("*--salvando a database--*")
    dataframe.to_parquet("data.parquet3")

def createPeriodDay(parquetPath: str):
    print("*--abrindo database--*")
    dataframe = pd.read_parquet(f'{parquetPath}.parquet3', engine="pyarrow")
    print("*--convertendo timestamp--*")
    lista = []
    dataframenumpy = dataframe.to_numpy()
    count = 0
    for linha in dataframenumpy:
        hour = linha[17].hour
        if hour >= 0 and hour <= 4:
            lista.append("dawn")
        elif hour > 4 and hour < 12:
            lista.append("morning")
        elif hour >= 12 and hour < 19:
            lista.append("evening")
        elif hour >= 19 and hour <= 23:
            lista.append("night")
    dataframe["period_day"] = lista
    print("*--salvando a database--*")
    dataframe.to_parquet("data2.parquet3")

def createLoginsPerId(parquetPath: str):
    print("*--abrindo database--*")
    dataframe = pd.read_parquet(f'{parquetPath}.parquet3', engine="pyarrow")
    print("*--convertendo timestamp--*")
    table = {}
    dataframenumpy = dataframe.to_numpy()
    count = 0
    for linha in dataframenumpy:
        if linha[1] in table.keys():
            table[linha[1]] += 1
        else:
            table[linha[1]] = 1
    dataframe = pd.DataFrame(table.items() ,columns=["account_id", "logins_amount"])
    print("*--salvando a database--*")
    dataframe.to_parquet("data3.parquet3")

def createAccountsPerDevice(parquetPath: str):
    print("*--abrindo database--*")
    dataframe = pd.read_parquet(f'{parquetPath}.parquet3', engine="pyarrow")
    print("*--convertendo timestamp--*")
    tableflag = {}
    table = {}
    dataframenumpy = dataframe.to_numpy()
    count = 0
    for linha in dataframenumpy:
        if linha[2] in tableflag.keys():
            if not (linha[1] in tableflag[linha[2]]):
                tableflag[linha[2]].append(linha[1])
                table[linha[2]] += 1
        else:
            tableflag[linha[2]] = [linha[1]]
            table[linha[2]] = 1
    dataframe = pd.DataFrame(table.items() ,columns=["account_id", "logins_amount"])
    print("*--salvando a database--*")
    dataframe.to_parquet("data4.parquet3")

def createAccountDevicesAmount(parquetPath: str):
    print("*--abrindo database--*")
    dataframe = pd.read_parquet(f'{parquetPath}.parquet3', engine="pyarrow")
    print("*--convertendo timestamp--*")
    tableflag = {}
    table = {}
    dataframenumpy = dataframe.to_numpy()
    count = 0
    for linha in dataframenumpy:
        if linha[1] in tableflag.keys():
            if not (linha[2] in tableflag[linha[1]]):
                tableflag[linha[1]].append(linha[2])
                table[linha[1]] += 1
        else:
            tableflag[linha[1]] = [linha[2]]
            table[linha[1]] = 1
    dataframe = pd.DataFrame(table.items() ,columns=["account_id", "logins_amount"])
    print("*--salvando a database--*")
    dataframe.to_parquet("data5.parquet3")

def deleteAnomaliesLines(parquetPath: str):
    pass

def deleteAnomaliesLines(parquetPath: str):
    pass

if (__name__ == "__main__"):
    # floatToBooleanColumn("logins-40", "is_emulator")
    # cleannerDatabase("logins")
    createAccountDevicesAmount("final")
    dataframe = pd.read_parquet(f'data5.parquet3', engine="pyarrow")
    datanumpy = dataframe.to_numpy()
    count = 0
    for linha in datanumpy:
        if (linha[1]) > 1:
            print(linha[0] ,linha[1])
            count += 1
    print(f"total de {count} linhas")