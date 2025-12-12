import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import os


class DataDownloader:
    def __init__(self, start_date="2025-01-01"):
        self.start_date = start_date
        self.end_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        self.ibovespa = None
        self.cambio = None

    def download_data(self):
        try:
            print(f"Baixando dados de {self.start_date} até {self.end_date}...")

            self.ibovespa = yf.download(
                "^BVSP", start=self.start_date, end=self.end_date
            )
            self.cambio = yf.download(
                "USDBRL=X", start=self.start_date, end=self.end_date
            )

            if self.ibovespa.empty or self.cambio.empty:
                raise ValueError(
                    "Os dados baixados estão vazios. Verifique sua conexão com a Internet e tente novamente."
                )

            print("Dados baixados com sucesso!")

        except Exception as e:
            print(f"Erro ao baixar os dados: {e}")

    def get_ibovespa(self):
        return self.ibovespa

    def get_cambio(self):
        return self.cambio

    def get_end_date(self):
        return self.end_date


class DataTransformer:

    def __init__(self, ibovespa, cambio):
        self.ibovespa_raw = ibovespa
        self.cambio_raw = cambio
        self.ibovespa_df = None
        self.cambio_df = None
        self.transformed_data = None

    def transform_data(self):
        try:
            print("Tratando os dados...")

            ibov = pd.DataFrame(self.ibovespa_raw)
            usd = pd.DataFrame(self.cambio_raw)

            # Flatten das colunas (corrige MultiIndex)
            ibov.columns = ibov.columns.get_level_values(0)
            usd.columns = usd.columns.get_level_values(0)

            # Selecionar colunas úteis
            colunas_validas = ["Close", "High", "Low", "Open", "Volume"]
            ibov_cols = [c for c in colunas_validas if c in ibov.columns]
            usd_cols = [c for c in colunas_validas if c in usd.columns]

            ibov = ibov[ibov_cols].copy()
            usd = usd[usd_cols].copy()

            # Renomear
            ren_ibov = {
                "Close": "Fechamento_IBOV",
                "High": "Maximo_IBOV",
                "Low": "Minimo_IBOV",
                "Open": "Abertura_IBOV",
                "Volume": "Volume_IBOV",
            }

            ren_usd = {
                "Close": "Fechamento_Cambio",
                "High": "Maximo_Cambio",
                "Low": "Minimo_Cambio",
                "Open": "Abertura_Cambio",
                "Volume": "Volume_Cambio",
            }

            ibov.rename(columns=ren_ibov, inplace=True)
            usd.rename(columns=ren_usd, inplace=True)

            # Merge pelo índice
            merged = pd.merge(
                ibov[["Fechamento_IBOV"]],
                usd[["Fechamento_Cambio"]],
                left_index=True,
                right_index=True,
            )

            # Resetar índice → coluna Date
            ibov = ibov.reset_index().rename(columns={"index": "Date"})
            usd = usd.reset_index().rename(columns={"index": "index"})
            merged = merged.reset_index().rename(columns={"index": "Date"})

            # Normalizar datas
            for d in [ibov, usd, merged]:
                d["Date"] = pd.to_datetime(d["Date"]).dt.normalize()

            # IBOV/USD
            merged["IBOV_USD"] = merged["Fechamento_IBOV"] / merged["Fechamento_Cambio"]

            # Salvar nos atributos
            self.ibovespa_df = ibov
            self.cambio_df = usd
            self.transformed_data = merged

            print("Dados tratados com sucesso!")

        except Exception as e:
            print(f"Erro ao tratar os dados: {e}")

    def get_transformed_data(self):
        return self.transformed_data

    def get_ibovespa(self):
        return self.ibovespa_df

    def get_cambio(self):
        return self.cambio_df


class DataPipeline:
    def __init__(self):
        self.downloader = DataDownloader()
        self.transformer = None
        self.output_dir = "Output"  # pasta padrão de saída

        # garante que a pasta exista
        os.makedirs(self.output_dir, exist_ok=True)

    def run(self):
        self.downloader.download_data()

        ibovespa = self.downloader.get_ibovespa()
        cambio = self.downloader.get_cambio()
        end_date = self.downloader.get_end_date()

        if ibovespa is not None and cambio is not None:

            self.transformer = DataTransformer(ibovespa, cambio)
            self.transformer.transform_data()

            df_ibov = self.transformer.get_ibovespa()
            df_usd = self.transformer.get_cambio()
            df_merged = self.transformer.get_transformed_data()

            if df_merged is not None:

                try:
                    print("Salvando arquivos...")

                    df_ibov.to_csv(
                        os.path.join(self.output_dir, f"IBOVESPA_{end_date}.csv"),
                        index=False,
                    )
                    df_usd.to_csv(
                        os.path.join(self.output_dir, f"USD_{end_date}.csv"),
                        index=False,
                    )
                    df_merged.to_csv(
                        os.path.join(self.output_dir, f"IBOVESPA_USD_{end_date}.csv"),
                        index=False,
                    )

                    print("Arquivos salvos com sucesso!")

                except Exception as e:
                    print(f"Erro ao salvar os dados: {e}")

            else:
                print("Erro: Falha no tratamento dos dados.")
        else:
            print("Erro: Dados não foram baixados corretamente.")


if __name__ == "__main__":
    pipeline = DataPipeline()
    pipeline.run()
