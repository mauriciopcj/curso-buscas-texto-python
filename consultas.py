import pymysql
import nltk


def frequencia_score(linhas):
    contagem = dict((linha[0], 0) for linha in linhas)
    for linha in linhas:
        contagem[linha[0]] += 1
    return contagem


def localizacao_score(linhas):
    localizacoes = dict((linha[0], 1000000) for linha in linhas)
    for linha in linhas:
        soma = sum(linha[1:])
        if soma < localizacoes[linha[0]]:
            localizacoes[linha[0]] = soma
    return localizacoes


def pesquisa(consulta):
    linhas, palavras_id = busca_mais_palavras(consulta)
    # scores = dict((linha[0], 0) for linha in linhas)
    # scores = frequencia_score(linhas)
    scores = localizacao_score(linhas)
    scores_ordenado = sorted([(score, url) for (url, score) in scores.items()], reverse=False)
    for (score, idurl) in scores_ordenado[0:10]:
        print(f'{score} \t {get_url(idurl)}')


def get_url(idurl):
    retorno = ''
    conexao = pymysql.connect(
        host='localhost', user='root', passwd='!@Jfpb2019', db='indice')
    cursor = conexao.cursor()
    cursor.execute(f'SELECT url FROM urls WHERE idurl = {idurl}')
    if cursor.rowcount > 0:
        retorno = cursor.fetchone()[0]
    cursor.close()
    conexao.close()
    return retorno


def busca_mais_palavras(consulta):
    lista_campos = 'p1.idurl'
    lista_tabelas = ''
    lista_clausulas = ''
    palavras_id = []

    palavras = consulta.split(' ')
    numero_tabela = 1
    for palavra in palavras:
        id_palavra = get_id_palavra(palavra)
        if id_palavra > 0:
            palavras_id.append(id_palavra)
            if numero_tabela > 1:
                lista_tabelas += ', '
                lista_clausulas += ' AND '
                lista_clausulas += f'p{numero_tabela - 1}.idurl = p{numero_tabela}.idurl AND '
            lista_campos += f', p{numero_tabela}.localizacao'
            lista_tabelas += f'palavra_localizacao p{numero_tabela}'
            lista_clausulas += f'p{numero_tabela}.idpalavra = {id_palavra}'
            numero_tabela += 1
    consulta_completa = f'SELECT {lista_campos} FROM {lista_tabelas} WHERE {lista_clausulas}'

    conexao = pymysql.connect(
        host='localhost', user='root', passwd='!@Jfpb2019', db='indice')
    cursor = conexao.cursor()
    cursor.execute(consulta_completa)
    linhas = [linha for linha in cursor]

    cursor.close()
    conexao.close()
    return linhas, palavras_id


def get_id_palavra(palavra):
    retorno = -1
    stemmer = nltk.stem.RSLPStemmer()
    conexao = pymysql.connect(
        host='localhost', user='root', passwd='!@Jfpb2019', db='indice')
    cursor = conexao.cursor()
    cursor.execute(
        f'SELECT idpalavra FROM palavras WHERE palavra = "{stemmer.stem(palavra)}"')
    if cursor.rowcount > 0:
        retorno = cursor.fetchone()[0]
    cursor.close()
    conexao.close()
    return retorno


def busca_uma_palavra(palavra):
    id_palavra = get_id_palavra(palavra)
    conexao = pymysql.connect(
        host='localhost', user='root', passwd='!@Jfpb2019', db='indice')
    cursor = conexao.cursor()
    cursor.execute(
        'SELECT urls.url FROM palavra_localizacao plc INNER JOIN' +
        f' urls ON plc.idurl = urls.idurl WHERE plc.idpalavra = {id_palavra}')
    paginas = set()
    for url in cursor:
        # print(url[0])
        paginas.add(url[0])
    print('Páginas encontradas: ' + str(len(paginas)))
    for url in paginas:
        print(url)
    cursor.close()
    conexao.close()


# print(get_id_palavra('programação'))

# busca_uma_palavra('programação')

# linhas, palavras_id = busca_mais_palavras('python programação')

# print(linhas)
# print(palavras_id)

pesquisa('python programação')
