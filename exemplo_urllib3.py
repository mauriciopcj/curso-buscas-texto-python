import urllib3

http = urllib3.PoolManager()
pagina = http.request('GET', 'https://www.iaexpert.com.br/')
print(pagina.status)
print(pagina.data[0:50])
