r"""°°°
# Projeto - ANÁLISE DE TEXTO DE FONTES DESESTRUTURADAS E WEB 

## Análise dos dados imobiliários de São Paulo a partir de dados do site Quinto Andar

Feito por: Augusto Carneiro, Guilherme Rameh - 05/2024

------

## Proposta:

A partir do enunciado do projeto, o grupo se propõe a coletar dados por meio de web scrapping no site "Quinto Andar", e analisar o cenário imobilipario de São Paulo a partir deles.
°°°"""
# |%%--%%| <dsEkOG54QM|fMdvy8iD4I>
r"""°°°
## Coleta dos Dados
A página principal de busca de apartamentos do Quinto Andar lista apartamentos disponíveis, mas se você não clicar nele, faltam informações que achamos importantes. Assim, primeiro salvaremos o máximo de links para apartamentos possíveis, e então acessamos cada um para pegar as informações que queremos usar. Essas informações são transformadas em um DataFrame da biblioteca Pandas, e salvo em um arquivo do tipo *parquet* para não precisarmos executar a busca de dados a cada inicialização deste arquivo.
°°°"""
# |%%--%%| <fMdvy8iD4I|6iOoe9ZA2c>

from selenium import webdriver
from selenium.common.exceptions import StaleElementReferenceException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver import ActionChains

chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument('--headless') # ensure GUI is off
driver = webdriver.Chrome(options=chrome_options)
driver.set_page_load_timeout(60)
url = 'https://www.quintoandar.com.br/alugar/imovel/sao-paulo-sp-brasil'

# |%%--%%| <6iOoe9ZA2c|MjAEX9Gim3>

driver.get(url)
driver.implicitly_wait(10)

wait = WebDriverWait(driver, timeout=5)

# |%%--%%| <MjAEX9Gim3|59fNpAJ9hj>
r"""°°°
Na célula abaixo, a busca e armazenamento dos links de cada apartamento. Isso é necessário pois clicamos no botão de "mostrar mais" para obtermos apartamentos antes inacessíveis.
°°°"""
# |%%--%%| <59fNpAJ9hj|KXoiItY1tP>

horas = []

for i in range(100):
    try:
        horas = driver.find_elements(By.XPATH, '//main/section[2]/div/div')
        wait.until(EC.element_to_be_clickable((By.XPATH, '//main/section[2]/div/div[last()-2]/button')))
        horas[-3].click()
        wait.until(EC.staleness_of(horas[-3]))

    except Exception as err:
        print(f'\nse fudeu em {i}: {err = }\n')

        if isinstance(err, StaleElementReferenceException):
            print("Attempting to recover from StaleElementReferenceException")
            wait.until(EC.element_to_be_clickable((By.XPATH, '//main/section[2]/div/div[last()-2]/button')))
        else:
            raise err

    print(f'list size on iteration {i}: {len(horas)}')

# |%%--%%| <KXoiItY1tP|PQx8kR236D>

links = []
# os divs de interesse vao desde o segundo ate o anteantepenultimo
for i, div in enumerate(horas[1:-4]):
    current_link = div.find_element(by=By.TAG_NAME, value='a').get_property('href')
    print(f'link at iteration {i} {current_link}')
    links.append(current_link)

# |%%--%%| <PQx8kR236D|lrjkC5ZuKv>
r"""°°°
para cada link, obter aas informações de interesse e guardar em um DF
°°°"""
# |%%--%%| <lrjkC5ZuKv|uQVCf2c1nq>

data_info = {
    'suite_area'    : {
        'xpath' : '//main/section/div/div[1]/div/div[3]/section/div/div[3]/div/div/div[1]/div/div/p',
        'type'  : 'numeric'
    },
    'street'        : {
        'xpath' : '//main/section/div/div[1]/div/div[2]/div/div/div/div[1]/div/h4',
        'type'  : 'text'
    },
    'neighborhood'  : {
        'xpath' : '//main/section/div/div[1]/div/div[2]/div/div/div/div[1]/small',
        'type'  : 'text'
    },
    'condominium'   : {
        'xpath' : '//main/section/div/div[1]/div/div[3]/section/div/div[2]/div/ul/li[2]/div/div/p',
        'type'  : 'numeric'
    },
    'tax'           : {
        'xpath' : '//main/section/div/div[1]/div/div[3]/section/div/div[2]/div/ul/li[3]/div/div/p',
        'type'  : 'numeric'
    },
    'asking_price'  : {
        'xpath' : '//main/section/div/div[1]/div/div[3]/section/div/div[2]/div/ul/li[1]/div/div/p',
        'type'  : 'numeric'
    }
}

# |%%--%%| <uQVCf2c1nq|y4LchbaYdx>

import pandas as pd


delta_y = 800
stock = []

for i, link in enumerate(links):
    driver.get(link)
    driver.implicitly_wait(10)
    ActionChains(driver) \
        .scroll_by_amount(0, delta_y) \
        .perform()

    try:
        raw_suite_data = {
            key: driver.find_element(by=By.XPATH, value=info['xpath']).text for key, info in data_info.items()
        }

        print(f'raw data on iteration {i} {raw_suite_data}')
        
        treated_suite_data = {
            key: int('0'+''.join(filter(str.isdecimal, value))) if data_info[key]['type'] == 'numeric' else value for key, value in raw_suite_data.items()
        }
        
        print(f'treated data on iteration {i} {treated_suite_data}')
        
        stock.append(treated_suite_data)
    except: continue

# |%%--%%| <y4LchbaYdx|7ZxDlLPgXi>

df = pd.DataFrame(stock)
df

# |%%--%%| <7ZxDlLPgXi|83ofcZvUB6>

df.to_parquet('stock.parquet')

# |%%--%%| <83ofcZvUB6|IK8cZ4qrd6>
r"""°°°
## Análise dos dados

Agora exploraremos os dados que conseguimos, analisando relação entre localidade, preço do metro quadrado e ???

Começamos lendo o arquivo de dados:
°°°"""
# |%%--%%| <IK8cZ4qrd6|p8YfMvCARX>

import pandas as pd
import matplotlib.pyplot as plt

# |%%--%%| <p8YfMvCARX|eBDChoZRtk>

df = pd.read_parquet('stock.parquet')
df

# |%%--%%| <eBDChoZRtk|e5dSpNC3tP>
r"""°°°
Verificamos se existem entradas duplicadas
°°°"""
# |%%--%%| <e5dSpNC3tP|LfH6wty4c6>

df[df.duplicated(keep=False)]

# |%%--%%| <LfH6wty4c6|9QwrhZ2eTF>

df.drop_duplicates(inplace=True)

# |%%--%%| <9QwrhZ2eTF|nUaavFbzuL>
r"""°°°
Aqui verificamos a escrita dos bairros. Percebemos que algumas entradas diferentes eram causadas por entradas com e sem acentos, como "America" e "América". Usamos a biblioteca *Levenshtein* para identificar entradas com diferença de 1 ou dois caracteres, e usamos do input do usuário para definir qual estpa certo e alterá-lo no df original. Há casos que ambôs estão corretos, e para isso, lidamos com um terceiro input que pula aquela alteração, e mantém ambas as entradas.
°°°"""
# |%%--%%| <nUaavFbzuL|qDyyRpcwZq>

import Levenshtein

neighborhood_names = df['neighborhood'].unique()

similar_neighborhoods = []
already_checked = []

# |%%--%%| <qDyyRpcwZq|ojFUbkLw9j>

for i in range(len(neighborhood_names)):
    for j in range(i+1, len(neighborhood_names)):
        distance = Levenshtein.distance(neighborhood_names[i], neighborhood_names[j])
        if distance <= 2:
            similar_neighborhoods.append((neighborhood_names[i], neighborhood_names[j]))

for neighborhood_pair in similar_neighborhoods:
    if neighborhood_pair in already_checked:
        continue
    print(neighborhood_pair)
    user_input = input("Which neighborhood is correct? Enter the correct neighborhood name: ")
    if user_input == "":
        break
    else :
        user_input = int(user_input)
    correct_neighborhood = neighborhood_pair[0] if user_input==1 else neighborhood_pair[1] if user_input == 2 else None
    wrong_neighborhood = neighborhood_pair[1] if neighborhood_pair[0] == correct_neighborhood else neighborhood_pair[0]
    if correct_neighborhood:
        print(f"Corrigindo {wrong_neighborhood} para {correct_neighborhood}...")
        df.loc[df['neighborhood'] == wrong_neighborhood, 'neighborhood'] = correct_neighborhood
    else:
        print("Invalid input. Skipping...")
        already_checked.append(neighborhood_pair)

# |%%--%%| <ojFUbkLw9j|FisslN9vGc>

df['price_per_sqm'] = df['asking_price'] / df['suite_area']
neighborhood_avg_price = df.groupby('neighborhood')['price_per_sqm'].mean()
neighborhood_avg_price.sort_values(ascending=False).head(25).plot(kind='bar', figsize=(15, 10))

# |%%--%%| <FisslN9vGc|1Jix8ORjL3>


