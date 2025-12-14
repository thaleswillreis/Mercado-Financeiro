import yfinance as yf
from .config import DEFAULT_START_DATE, get_end_date
from .utils.logging import get_logger


logger = get_logger(__name__)


class DataDownloader:
    def __init__(self, start_date: str = DEFAULT_START_DATE):
        self.start_date = start_date
        self.end_date = get_end_date()

    def download(self) -> dict:
        logger.info(
            "Baixando dados de %s até %s", self.start_date, self.end_date
        )

        ibov = yf.download("^BVSP", start=self.start_date, end=self.end_date)
        usd = yf.download("USDBRL=X", start=self.start_date, end=self.end_date)

        if ibov.empty or usd.empty:
            raise ValueError("Dados baixados estão vazios")

        logger.info("Download concluído com sucesso")

        return {
            "ibovespa": ibov,
            "cambio": usd,
            "end_date": self.end_date,
        }
