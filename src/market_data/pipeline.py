from .downloader import DataDownloader
from .transformer import DataTransformer
from .config import get_output_dir
from .utils.logging import get_logger


logger = get_logger(__name__)


class MarketDataPipeline:
    def run(self):
        downloader = DataDownloader()
        transformer = DataTransformer()

        data = downloader.download()
        transformed = transformer.transform(
            data["ibovespa"], data["cambio"]
        )

        output_dir = get_output_dir()
        end_date = data["end_date"]

        transformed["ibovespa"].to_csv(
            output_dir / f"IBOVESPA_{end_date}.csv", index=False
        )
        transformed["cambio"].to_csv(
            output_dir / f"USD_{end_date}.csv", index=False
        )
        transformed["merged"].to_csv(
            output_dir / f"IBOVESPA_USD_{end_date}.csv", index=False
        )

        logger.info("Arquivos salvos com sucesso em %s", output_dir)
