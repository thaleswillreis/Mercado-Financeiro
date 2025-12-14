import pandas as pd
from .utils.logging import get_logger


logger = get_logger(__name__)


class DataTransformer:
    def transform(self, ibovespa: pd.DataFrame, cambio: pd.DataFrame) -> dict:
        logger.info("Iniciando transformação dos dados")

        ibov = ibovespa.copy()
        usd = cambio.copy()

        ibov.columns = ibov.columns.get_level_values(0)
        usd.columns = usd.columns.get_level_values(0)

        cols = ["Close", "High", "Low", "Open", "Volume"]
        ibov = ibov[[c for c in cols if c in ibov.columns]]
        usd = usd[[c for c in cols if c in usd.columns]]

        ibov.rename(
            columns={
                "Close": "Fechamento_IBOV",
                "High": "Maximo_IBOV",
                "Low": "Minimo_IBOV",
                "Open": "Abertura_IBOV",
                "Volume": "Volume_IBOV",
            },
            inplace=True,
        )

        usd.rename(
            columns={
                "Close": "Fechamento_Cambio",
                "High": "Maximo_Cambio",
                "Low": "Minimo_Cambio",
                "Open": "Abertura_Cambio",
                "Volume": "Volume_Cambio",
            },
            inplace=True,
        )

        merged = pd.merge(
            ibov[["Fechamento_IBOV"]],
            usd[["Fechamento_Cambio"]],
            left_index=True,
            right_index=True,
        )

        for df in [ibov, usd, merged]:
            df.reset_index(inplace=True)
            df.rename(columns={"index": "Date"}, inplace=True)
            df["Date"] = pd.to_datetime(df["Date"]).dt.normalize()

        merged["IBOV_USD"] = (
            merged["Fechamento_IBOV"] / merged["Fechamento_Cambio"]
        )

        logger.info("Transformação concluída")

        return {
            "ibovespa": ibov,
            "cambio": usd,
            "merged": merged,
        }
