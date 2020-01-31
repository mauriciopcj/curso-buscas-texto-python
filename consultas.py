import pymysql
import nltk

senha_db = 'senha'


def normaliza_maior(notas):
    menor = 0.00001
    maximo = max(notas.values())
    if maximo == 0:
        maximo = menor
    return dict([(id_nota, float(nota) / maximo) for (id_nota, nota) in notas.items()])


def normaliza_menor(notas):
    menor = 0.00001
    minimo = min(notas.values())
    return dict([(id_nota, float(minimo) / max(menor, nota)) for (id_nota, nota) in notas.items()])


def calcula_page_rank(iteracoes):
    conexao = pymysql.connect(
        host='localhost', user='root', passwd=senha_db, db='indice', autocommit=True)
    cursor_limpa_tabela = conexao.cursor()
    cursor_limpa_tabela.execute('DELETE FROM page_rank')
    cursor_limpa_tabela.execute('INSERT INTO page_rank SELECT idurl, 1.0 FROM urls')

    for i in range(iteracoes):
        print("Iteraçao " + str(i + 1))
        cursor_url = conexao.cursor()
        cursor_url.execute('SELECT idurl FROM urls')

        for url in cursor_url:
            pr = 0.15
            cursor_links = conexao.cursor()
            cursor_links.execute(
                f'SELECT idurl_origem FROM url_ligacao WHERE idurl_destino = {url[0]}')

            for link in cursor_links:
                cursor_page_rank = conexao.cursor()
                cursor_page_rank.execute(
                    f'SELECT nota FROM page_rank WHERE idurl = {link[0]}')
                link_page_rank = cursor_page_rank.fetchone()[0]
                cursor_quantidade = conexao.cursor()
                cursor_quantidade.execute(
                    f'SELECT COUNT(*) FROM url_ligacao WHERE idurl_origem = {link[0]}')
                link_quantidade = cursor_quantidade.fetchone()[0]
                pr += 0.85 * (link_page_rank / link_quantidade)
                cursor_quantidade.close()
                cursor_page_rank.close()

            cursor_atualiza = conexao.cursor()
            cursor_atualiza.execute(
                f'UPDATE page_rank SET nota = {pr} WHERE idurl = {url[0]}')
            cursor_atualiza.close()
            cursor_links.close()

        cursor_url.close()

    cursor_limpa_tabela.close()
    conexao.close()


def frequencia_score(linhas):
    contagem = dict((linha[0], 0) for linha in linhas)
    for linha in linhas:
        contagem[linha[0]] += 1
    return normaliza_maior(contagem)


def localizacao_score(linhas):
    localizacoes = dict((linha[0], 1000000) for linha in linhas)
    for linha in linhas:
        soma = sum(linha[1:])
        if soma < localizacoes[linha[0]]:
            localizacoes[linha[0]] = soma
    return normaliza_menor(localizacoes)


def distancia_score(linhas):
    if len(linhas[0]) <= 2:
        return dict([(linha[0], 1.0) for linha in linhas])
    distancias = dict([(linha[0], 1000000) for linha in linhas])
    for linha in linhas:
        dist = sum([abs(linha[i] - linha[i - 1]) for i in range(2, len(linha))])
        if dist < distancias[linha[0]]:
            distancias[linha[0]] = dist
    return normaliza_menor(distancias)


def constagens_links_score(linhas):
    contagem = dict((linha[0], 1) for linha in linhas)
    conexao = pymysql.connect(
        host='localhost', user='root', passwd=senha_db, db='indice')
    cursor = conexao.cursor()
    for i in contagem:
        cursor.execute(f'SELECT COUNT(*) FROM url_ligacao WHERE idurl_destino = {i}')
        contagem[i] = cursor.fetchone()[0]
    cursor.close()
    conexao.close()
    return normaliza_maior(contagem)


def page_rank_score(linhas):
    page_ranks = dict((linha[0], 1.0) for linha in linhas)
    conexao = pymysql.connect(
        host='localhost', user='root', passwd=senha_db, db='indice')
    cursor = conexao.cursor()
    for i in page_ranks:
        cursor.execute(f'SELECT nota FROM page_rank WHERE idurl = {i}')
        page_ranks[i] = cursor.fetchone()[0]
    cursor.close()
    conexao.close()
    return normaliza_maior(page_ranks)


def texto_link_score(linhas, palavras_id):
    contagem = dict((linha[0], 0) for linha in linhas)
    conexao = pymysql.connect(
        host='localhost', user='root', passwd=senha_db, db='indice')
    for palavra_id in palavras_id:
        cursor = conexao.cursor()
        cursor.execute('SELECT ul.idurl_origem, ul.idurl_destino ' +
                       'FROM url_palavra up INNER JOIN url_ligacao ul ' +
                       'ON up.idurl_ligacao = ul.idurl_ligacao ' +
                       f'WHERE up.idpalavra = {palavra_id}')
        for (id_url_origem, id_url_destino) in cursor:
            if id_url_destino in contagem:
                cursor_rank = conexao.cursor()
                cursor_rank.execute(f'SELECT nota FROM page_rank WHERE idurl = {id_url_origem}')
                pr = cursor_rank.fetchone()[0]
                contagem[id_url_destino] += pr
                cursor_rank.close()
        cursor.close()
    conexao.close()
    return normaliza_maior(contagem)


def pesquisa(consulta):
    linhas, palavras_id = busca_mais_palavras(consulta)
    # scores = dict((linha[0], 0) for linha in linhas)
    # scores = frequencia_score(linhas)
    # scores = localizacao_score(linhas)
    # scores = distancia_score(linhas)
    # scores = constagens_links_score(linhas)
    # scores = page_rank_score(linhas)
    scores = texto_link_score(linhas, palavras_id)
    scores_ordenado = sorted([(score, url) for (url, score) in scores.items()], reverse=True)
    for (score, idurl) in scores_ordenado[0:10]:
        print(f'{round(score, 8)} \t {get_url(idurl)}')


def pesquisa_peso(consulta):
    linhas, palavras_id = busca_mais_palavras(consulta)
    total_scores = dict((linha[0], 0) for linha in linhas)
    pesos = [
        (1.0, frequencia_score(linhas)),
        (1.0, localizacao_score(linhas)),
        (1.0, distancia_score(linhas)),
        (1.0, constagens_links_score(linhas)),
        (1.0, page_rank_score(linhas)),
        (1.0, texto_link_score(linhas, palavras_id))
    ]
    for (peso, scores) in pesos:
        for url in total_scores:
            total_scores[url] += peso * scores[url]
    total_scores = normaliza_maior(total_scores)
    scores_ordenado = sorted([(score, url) for (url, score) in total_scores.items()], reverse=True)
    for (score, idurl) in scores_ordenado[0:10]:
        print(f'{round(score, 8)} \t {get_url(idurl)}')


def get_url(idurl):
    retorno = ''
    conexao = pymysql.connect(
        host='localhost', user='root', passwd=senha_db, db='indice')
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
        host='localhost', user='root', passwd=senha_db, db='indice')
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
        host='localhost', user='root', passwd=senha_db, db='indice')
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
        host='localhost', user='root', passwd=senha_db, db='indice')
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

# pesquisa('python programação')

pesquisa_peso('python programação')

# calcula_page_rank(20)
