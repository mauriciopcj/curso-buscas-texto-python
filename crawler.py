import urllib3
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import re
import nltk
import pymysql


def insere_palavra_localizacao(id_url, id_palavra, localizacao):
    conexao = pymysql.connect(
        host='localhost', user='root', passwd='!@Jfpb2019',
        db='indice', autocommit=True)
    cursor = conexao.cursor()
    cursor.execute(
        'insert into palavra_localizacao (idurl, idpalavra, localizacao) ' +
        f'values ({id_url}, {id_palavra}, {localizacao})')
    id_palavra_localizacao = cursor.lastrowid
    cursor.close()
    conexao.close()
    return id_palavra_localizacao


def insere_palavra(palavra):
    conexao = pymysql.connect(
        host='localhost',
        user='root',
        passwd='!@Jfpb2019',
        db='indice',
        autocommit=True,
        use_unicode=True,
        charset='utf8mb4'
    )
    cursor = conexao.cursor()
    cursor.execute(f'insert into palavras (palavra) values ("{palavra}")')
    id_palavra = cursor.lastrowid
    cursor.close()
    conexao.close()
    return id_palavra


def palavra_indexada(palavra):
    retorno = -1  # não existe a palavra
    conexao = pymysql.connect(host='localhost', user='root', passwd='!@Jfpb2019', db='indice')
    cursor = conexao.cursor()
    cursor.execute(f'select idpalavra from palavras where palavra = "{palavra}"')
    if cursor.rowcount > 0:
        # print('Palavra já cadastrada')
        retorno = cursor.fetchone()[0]
    # else:
    #     print('Palavra não cadastrada')
    cursor.close()
    conexao.close()
    return retorno


def insere_pagina(url):
    conexao = pymysql.connect(host='localhost', user='root', passwd='!@Jfpb2019', db='indice', autocommit=True)
    cursor = conexao.cursor()
    cursor.execute(f'insert into urls (url) values ("{url}")')
    id_pagina = cursor.lastrowid
    cursor.close()
    conexao.close()
    return id_pagina


def pagina_indexada(url):
    retorno = -1  # não existe a página
    conexao = pymysql.connect(
        host='localhost', user='root', passwd='!@Jfpb2019', db='indice')
    cursor_url = conexao.cursor()
    cursor_url.execute(f'select idurl from urls where url = "{url}"')

    if cursor_url.rowcount > 0:
        # print('Url encontrada')
        id_url = cursor_url.fetchone()[0]
        cursor_palavra = conexao.cursor()
        cursor_palavra.execute(
            f'SELECT idurl FROM palavra_localizacao WHERE idurl = "{id_url}"')
        if cursor_palavra.rowcount > 0:
            # print('Url com palavras')
            retorno = -2  # existe a pagina com palavras cadastradas
        else:
            print('Url sem palavras')
            retorno = id_url  # existe a pagina sem palavras, retorna id da pagina

        cursor_palavra.close()
    # else:
    #     print('Url não cadastrada')

    cursor_url.close()
    conexao.close()

    return retorno


def separa_palavras(texto):
    """ recebe um texto do tipo string e retorna o radical das palavras"""
    stop = nltk.corpus.stopwords.words('portuguese')
    stemmer = nltk.stem.RSLPStemmer()
    splitter = re.compile('\\W+')
    lista_palavras = []
    lista = [p for p in splitter.split(texto) if p != '']
    for p in lista:
        if p.lower() not in stop:
            if len(p) > 1:
                lista_palavras.append(stemmer.stem(p).lower())
    return lista_palavras


def get_texto(sopa):
    for tags in sopa(['script', 'style']):
        tags.decompose()
    return ' '.join(sopa.stripped_strings)


def indexador(url, sopa):
    indexada = pagina_indexada(url)
    if indexada == -2:
        print('Url já indexada')
        return
    elif indexada == -1:
        id_nova_pagina = insere_pagina(url)
    elif indexada > 0:
        id_nova_pagina = indexada

    print('Indexando ' + url)

    texto = get_texto(sopa)
    palavras = separa_palavras(texto)
    for i in range(len(palavras)):
        palavra = palavras[i]
        id_palavra = palavra_indexada(palavra)
        if id_palavra == -1:
            id_palavra = insere_palavra(palavra)
        insere_palavra_localizacao(id_nova_pagina, id_palavra, i)


def crawl(paginas, profundidade):
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    for i in range(profundidade):
        novas_paginas = set()
        for pagina in paginas:
            http = urllib3.PoolManager()
            try:
                dados_pagina = http.request('GET', pagina)
                sopa = BeautifulSoup(dados_pagina.data, 'html.parser')
                indexador(pagina, sopa)
                links = sopa.find_all('a')
                contador = 1

                for link in links:
                    if 'href' in link.attrs:
                        url = urljoin(pagina, str(link.get('href')))
                        if url.find("'") != -1:
                            continue
                        url = url.split('#')[0]
                        if url[0:4] == 'http':
                            novas_paginas.add(url)

                        contador += 1

                paginas = novas_paginas
                print(contador)
            except():
                print('Erro ao abrir a página' + pagina)
                continue


listapaginas = ['https://pt.wikipedia.org/wiki/Linguagem_de_programa%C3%A7%C3%A3o']
crawl(listapaginas, 2)

# print(separa_palavras('Este lugar é apavorante!'))

# print(pagina_indexada('teste'))

# insere_pagina('teste2')

# print(palavra_indexada('linguagem'))

# insere_palavra('teste2')

# insere_palavra_localizacao(1, 2, 50)
