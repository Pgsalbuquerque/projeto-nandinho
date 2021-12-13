import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

def graficoEmulador(parquetPath):
    """
    Função: O aplicativo recebe muitos acessos de emuladores?
    """
    dataframe = pd.read_parquet(f'{parquetPath}.parquet3', engine="pyarrow")
    colunaEmulador = dataframe["is_emulator"].values
    zeroSize = 0
    oneSize = 0
    for element in colunaEmulador:
        if element == 0:
            zeroSize+= 1
        if element == 1:
            oneSize += 1
    votos = [oneSize, zeroSize]
    opcoes = ['Emulador', "Não-emulador"]
    colunas = ['r', 'm']
    # Criando um gráfico
    plt.pie(votos, labels = opcoes, colors = colunas, startangle = 90, shadow = True, explode = (0.1, 0))
    plt.show()

def graficoLoja(parquetPath):
    """
    Função: O aplicativo é instalado na maioria das vezes pela loja oficial ou fora da loja?
    """
    dataframe = pd.read_parquet(f'{parquetPath}.parquet3', engine="pyarrow")
    colunaLoja = dataframe["is_from_official_store"].values
    zeroSize = 0
    oneSize = 0
    for element in colunaLoja:
        if element == 0:
            zeroSize+= 1
        if element == 1:
            oneSize += 1
    votos = [oneSize, zeroSize]
    opcoes = ['Loja oficial', "Outros meios"]
    explode = (0.1, 0.1) 
    plt.pie(votos, labels=opcoes, autopct='%1.2f%%', shadow=True, explode=explode)
    plt.legend(opcoes, loc=3)
    plt.axis('equal')
    plt.show()

def graficoIdadeDispositivo(parquetPath):
    """
    Função: Existem muitos acessos de dispositivos antigos?
    """
    dataframe = pd.read_parquet(f'{parquetPath}.parquet3', engine="pyarrow")
    colunaIdade = dataframe["device_age_ms"].values
    colunaLoja = dataframe["is_from_official_store"].values
    contadorAte6Meses = 0
    contadorAte1Ano = 0
    contadorAte3Anos = 0
    contadorAte5Anos = 0
    contadorAte10Anos = 0
    contadorMaisQue10Anos = 0
    ano = 31536000000.0
    for element in colunaIdade:
        if element < ano/2:
            contadorAte6Meses += 1
        elif element < ano:
            contadorAte1Ano += 1
        elif element < 3 * ano:
            contadorAte3Anos += 1
        elif element < 5 * ano:
            contadorAte5Anos += 1
        elif element <  10 * ano:
            contadorAte10Anos += 1
        elif element > 10 * ano:
            contadorMaisQue10Anos += 1
    grupos = ['Até 6 meses', '1 ano', '3 anos', '5 anos', '10 anos', 'mais de 10 anos']
    valores = [contadorAte6Meses, contadorAte1Ano, contadorAte3Anos, contadorAte5Anos, contadorAte10Anos, contadorMaisQue10Anos]
    explode = (0.1, 0.1, 0.1, 0.1, 0.1, 0.1) 
    plt.pie(valores, labels=grupos, autopct='%1.1f%%', shadow=True, explode=explode)
    plt.legend(grupos, loc=3)
    plt.axis('equal')
    plt.show()

def graficoIdadeLoja(parquetPath):
    """
    Função: Idade do dispositivo que não foi instalado na loja oficial?
    """
    dataframe = pd.read_parquet(f'{parquetPath}.parquet3', engine="pyarrow")
    dataframenumpy = dataframe.to_numpy()
    contadorAte6Meses = 0
    contadorAte1Ano = 0
    contadorAte3Anos = 0
    contadorAte5Anos = 0
    contadorAte10Anos = 0
    contadorMaisQue10Anos = 0
    ano = 31536000000.0
    for linha in dataframenumpy:
        if linha[5] == False:
            if linha[10] < ano/2:
                contadorAte6Meses += 1
            elif linha[10] < ano:
                contadorAte1Ano += 1
            elif linha[10] < 3 * ano:
                contadorAte3Anos += 1
            elif linha[10] < 5 * ano:
                contadorAte5Anos += 1
            elif linha[10] <  10 * ano:
                contadorAte10Anos += 1
            elif linha[10] > 10 * ano:
                contadorMaisQue10Anos += 1
    grupos = ['Até 6 meses', '1 ano', '3 anos', '5 anos', '10 anos', 'mais de 10 anos']
    valores = [contadorAte6Meses, contadorAte1Ano, contadorAte3Anos, contadorAte5Anos, contadorAte10Anos, contadorMaisQue10Anos]
    explode = (0.1, 0.1, 0.1, 0.1, 0.1, 0.1) 
    plt.pie(valores, labels=grupos, autopct='%1.1f%%', shadow=True, explode=explode)
    plt.legend(grupos, loc=3)
    plt.axis('equal')
    plt.show()

def graficoRootFakeLocation(parquetPath):
    """
    Função: Idade do dispositivo que não foi instalado na loja oficial?
    """
    dataframe = pd.read_parquet(f'{parquetPath}.parquet3', engine="pyarrow")
    dataframenumpy = dataframe.to_numpy()
    fakeLocationPlusRoot = 0
    fakeLocationNotRoot = 0
    rootNotFakeLocation = 0
    for linha in dataframenumpy:
        if linha[7] == True and linha[9] == True:
            fakeLocationPlusRoot += 1
        if linha[7] == True and linha[9] == False:
            fakeLocationNotRoot += 1
        if linha[7] == False and linha[9] == True:
            rootNotFakeLocation += 1
    grupos = ["Root + Fake Location", "Root not fake Location", "Fake location not root"]
    valores = [fakeLocationPlusRoot, fakeLocationNotRoot, rootNotFakeLocation]
    explode = (0.1, 0.1, 0.1) 
    plt.pie(valores, labels=grupos, autopct='%1.1f%%', shadow=True, explode=explode)
    plt.legend(grupos, loc=3)
    plt.axis('equal')
    plt.show()


graficoRootFakeLocation("final")
