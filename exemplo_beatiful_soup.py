from bs4 import BeautifulSoup
import urllib3

# Desabilitando warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

http = urllib3.PoolManager()
pagina = http.request('GET', 'https://www.iaexpert.com.br')

print(pagina.status)

sopa = BeautifulSoup(pagina.data, 'html.parser')

print(sopa)
print(sopa.title)
print(sopa.title.string)

links = sopa.find_all('a')
for link in links:
    print(link.get('href'))
    print(link.contents)
