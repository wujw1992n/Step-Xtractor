import requests
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
import time
from config.environment import wb_credentials,geckodriver_path

class ExtractorController:
    def __init__(self):
        self.cookies = self.extract_cookies()

    def create_driver(self) -> webdriver.firefox.webdriver.WebDriver:
        options = Options()
        p = webdriver.FirefoxProfile()
        p.set_preference("permissions.default.image", 2)
        options.add_argument("--headless")
        options.add_argument("start-maximized")
        options.add_argument("--disable-gpu")
        options.add_argument("--disable-extensions")
        options.add_argument("--no-sandbox")
        driver = webdriver.Firefox(executable_path=geckodriver_path,
                                   options=options,
                                   firefox_profile=p)
        return driver

    def login(self, driver:webdriver.firefox.webdriver.WebDriver, login:tuple):
        print("Making login, please wait a momment")
        driver.get(
            "https://wbcba.worldbank.org/loginpage/externalB2BLogin.jsp?TYPE=33554433&REALMOID=06-000cb946-1e25-1dbc-bcd0-28760ab10000&GUID=&SMAUTHREASON=0&METHOD=GET&SMAGENTNAME=$SM$vq1kOd2bcKxB%2fDW9nzsDo46cjnbrhu32CS8g5VtMsZgT0S4PRIeEYtP6GUzajg6E&TARGET=$SM$http%3a%2f%2fstep%2eworldbank%2eorg%2fsecure%2fr2%2fen%2fhome")  # login link
        time.sleep(5)  # waiting for the page to load
        email = '//*[@id="USER"]'  # insert email
        b_enviar_email = "/html/body/center/table[3]/tbody/tr[1]/td[3]/form/table/tbody/tr[5]/td/table/tbody/tr[4]/td[1]/div/input"  # Botão de email
        senha = '//*[@id="PASSWORD"]'  # insert password
        logar = '/html/body/center/table[3]/tbody/tr[1]/td[3]/form/table/tbody/tr[5]/td/table/tbody/tr[4]/td[1]/div/input'  # Botão de logar
        driver.find_elements_by_xpath(email)[0].send_keys(login[0])  # write the email
        time.sleep(5)  # waiting for the page to load
        driver.find_elements_by_xpath(b_enviar_email)[0].click()  # Submit the email
        time.sleep(5)  # waiting for the page to load
        driver.find_elements_by_xpath(senha)[0].send_keys(login[1])  # write the password
        time.sleep(1)  # ewaiting for the page to load
        driver.find_elements_by_xpath(logar)[0].click()  # Submit the password
        time.sleep(2)

        print("Login finished")

    def extract_cookies(self):
        driver = self.create_driver()
        self.login(driver, (wb_credentials['email'], wb_credentials['password']))
        new_cookies = ''
        for cookie in driver.get_cookies():
            new_cookies = new_cookies + str(cookie["name"]) + '=' + str(cookie["value"]) + ';'
        driver.quit()
        return new_cookies

    def get_data(self,url:str, timeout:int = 10):
        try:
            response = requests.get(url, headers={'Cookie': self.cookies})
            return response.json()
        except Exception as error:
            if timeout == 0:
                print("Error found: {} URL: {}".format(error,url))
                print("Timeout, disregarding the operation........")
                return {}
            else:
                print("Error found: {} URL: {}".format(error,url))
                print("Making logging again, {} remaining attempts........".format(timeout))
                self.cookies = self.extract_cookies()
                return self.get_data(url = url, timeout= timeout - 1)