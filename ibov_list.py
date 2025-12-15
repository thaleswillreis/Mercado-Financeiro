import os
import time
from typing import Optional
from selenium import webdriver
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager


class IbovListUpdater:

    def __init__(
        self, download_dir: str = "output", file_name: str = "IBOV_LIST.csv"
    ) -> None:

        self.download_dir: str = os.path.abspath(download_dir)
        self.file_name: str = file_name
        self.file_path: str = os.path.join(self.download_dir, self.file_name)
        self.driver: Optional[WebDriver] = None

        # Criar o diretório 'output' se não existir
        os.makedirs(self.download_dir, exist_ok=True)

        # Remover o arquivo antigo se existir
        if os.path.exists(self.file_path):
            os.remove(self.file_path)

    def setup_driver(self) -> None:
        # Configura o driver do Selenium
        options: Options = Options()
        options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_experimental_option(
            "prefs",
            {
                "download.default_directory": self.download_dir,
                "download.prompt_for_download": False,
                "download.directory_upgrade": True,
                "safebrowsing.enabled": True,
            },
        )

        self.driver = webdriver.Chrome(
            service=ChromeService(ChromeDriverManager().install()), options=options
        )

    def download_file(self) -> None:

        if not self.driver:
            raise RuntimeError("O driver não foi inicializado corretamente.")

        url: str = (
            "https://sistemaswebb3-listados.b3.com.br/indexPage/day/IBOV?language=pt-br"
        )
        self.driver.get(url)
        print("Acessando os dados...")
        time.sleep(10)  # Tempo para a página carregar

        try:
            download_button = self.driver.find_element(
                By.XPATH,
                '//*[@id="divContainerIframeB3"]/div/div[1]/form/div[2]/div/div[2]/div/div/div[1]/div[2]/p/a',
            )
            download_button.click()
            print("Executando o download dos dados...")
        except Exception as e:
            print(f"Erro ao acionar o botão de download no site de origem: {e}")

    def rename_downloaded_file(self) -> None:

        time.sleep(10)  # Ajuste o tempo conforme necessário
        for file in os.listdir(self.download_dir):
            if file.startswith("IBOVDia") and file.endswith(".csv"):
                os.rename(os.path.join(self.download_dir, file), self.file_path)
                print(
                    f"Salvando o arquivo '{self.file_name}' na pasta '{self.download_dir}'."
                )

    def update_ibov_list(self) -> None:

        self.setup_driver()
        self.download_file()
        self.rename_downloaded_file()
        if self.driver:
            self.driver.quit()
        print("Carteira teórica diária Ibovespa atualizada.")


if __name__ == "__main__":
    updater = IbovListUpdater(download_dir="output", file_name="IBOV_LIST.csv")
    updater.update_ibov_list()
