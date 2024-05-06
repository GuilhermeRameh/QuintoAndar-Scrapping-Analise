from selenium import webdriver
from selenium.common.exceptions import StaleElementReferenceException
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
# driver = webdriver.Chrome()
# driver.set_page_load_timeout(60)

chrome_options = webdriver.ChromeOptions()
chrome_options.add_argument('--headless') # ensure GUI is off
driver = webdriver.Chrome(options=chrome_options)
driver.set_page_load_timeout(60)
url = 'https://www.quintoandar.com.br/alugar/imovel/sao-paulo-sp-brasil'
driver.get(url)
driver.implicitly_wait(10)

wait = WebDriverWait(driver, timeout=2)


#%%
horas = []

for i in range(10):
    try:
        horas = driver.find_elements(By.XPATH, '//main/section[2]/div/div')
        wait.until(EC.element_to_be_clickable((By.XPATH, '//main/section[2]/div/div[last()-2]/button')))
        horas[-3].click()
        driver.implicitly_wait(5)
    except Exception as err:
        print(f'se fudeu em {i}\n {err = }')

        if isinstance(err, StaleElementReferenceException):
            print("Attempting to recover from StaleElementReferenceException")
            wait.until(EC.element_to_be_clickable((By.XPATH, '//main/section[2]/div/div[last()-2]/button')))
        else:
            raise err

    print(f'list size on iteration {i}: {len(horas)}')
