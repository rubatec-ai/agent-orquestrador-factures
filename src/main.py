import logging
import os
import time as t
import io
from datetime import datetime

from src.api_extractors.openai_extractor import AIExtractor
from src.config import ConfigurationManager
from src.invoice_orchestrator.classes.invoice_orchestrator import InvoiceOrchestrator
from src.invoice_orchestrator.classes.problem import InvoiceProblem
from src.io_methods import IOHandler
from src.transform import Transformer

import warnings

# Import your API clients
from src.api_extractors.gmail_manager import GmailManager
from src.api_extractors.drive_manager import DriveManager
from src.api_extractors.ocr_extractor import GoogleOCRExtractor
from src.api_extractors.sheets_manager import GoogleSheetsManager


class MainProcess:
    """
    Orchestrates the main execution process.

    Attributes:
        _config (ConfigurationManager): Instance of configuration settings.
        _io (IOHandler): Input/output handler.
        _logger (Logger): Logger for tracking execution.
        _data_model (dict): Transformed data model tables.
        _problem (object): Placeholder for problem-specific logic.
    """

    def __init__(self):
        self._config = ConfigurationManager()
        self._io = IOHandler(self._config)
        self._logger, self._log_stream = self._initialize_logger()

        self._gmail_manager = GmailManager(config=self._config)
        self._drive_manager = DriveManager(config=self._config)
        self._sheets_manager = GoogleSheetsManager(config=self._config)
        self._ocr_extractor = GoogleOCRExtractor(config=self._config)
        self._openai_extractor = AIExtractor(
            model=self._config.agent_model,
            api_key=self._config.agent_api_key,
            temperature=self._config.agent_temperature,
            max_tokens=self._config._agent_max_tokens,
            logger=self._logger
        )
        self._clean_inputs = {}
        self._data_model = {}
        self._problem = None
        self._solution = {}

    def _initialize_logger(self) ->  logging.Logger:
        """
        Configure the logger instance.

        Returns:
            tuple: Configured logger and optional StringIO stream for logs.
        """
        logger = logging.getLogger("MainProcess")
        log_level = logging.DEBUG if self._config.logger_debug else logging.INFO
        logging.basicConfig(
            level=log_level,
            format="%(asctime)-12s %(name)-12s %(levelname)-8s %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )

        logging.getLogger("httpcore.http11").setLevel(logging.WARNING)
        logging.getLogger("httpcore.connection").setLevel(logging.WARNING)
        logging.getLogger("googleapiclient.discovery").setLevel(logging.WARNING)
        logging.getLogger("googleapiclient.discovery_cache").setLevel(logging.WARNING)

        return logger

    def _setup_file_handler(self) -> logging.FileHandler:
        """
        Setup file logging handler.

        Returns:
            logging.FileHandler: Configured file handler.
        """
        current_datetime = datetime.now().strftime('%Y%m%d_%H%M%S')
        log_file = os.path.join(self._config.logs_directory, f"log_{current_datetime}.log")
        file_handler = logging.FileHandler(filename=log_file)
        self._logger.addHandler(file_handler)
        return file_handler

    def run_etl(self) -> None:
        """
        Execute the ETL process (Extract, Transform, Load).

        1) Extract data from Gmail, Drive, OCR, Sage, or Sheets.
        2) Transform the data via Transformer.
        3) Load or export the resulting data model if configured.
        """
        # --------------------------------------------------------------------
        # 1) Instantiate the necessary clients to extract current data
        # --------------------------------------------------------------------

        extractors = {
            'gmail': self._gmail_manager,
            'drive': self._drive_manager,
            'sheets': self._sheets_manager
        }

        for name, extractor in extractors.items():
            extractor.extract()
            for key, df in extractor.clean_inputs.items():
                self._clean_inputs[key] = df

        # --------------------------------------------------------------------
        # 2) Transform the extracted data
        # --------------------------------------------------------------------
        # The Transformer might combine or clean the data from all sources.
        transformer = Transformer(
            config=self._config,
            io_handler=self._io,
            logger=self._logger,
            clean_inputs=self._clean_inputs
        )

        self._data_model = transformer.run()
        self._logger.info(f"Data model generated with keys: {list(self._data_model.keys())}")

        # --------------------------------------------------------------------
        # 4) Load / Export the transformed data (optional)
        # --------------------------------------------------------------------
        if self._config.export_etl:
            self._io.write_data_model(data_model=self._data_model, logger=self._logger)

    def run_solver(self) -> None:
        """
        Execute the solver logic.

        Integrates the Problem, Agent, and SemanticSearch classes to perform the semantic search.
        """
        if not self._config.run_etl:
            # Load the data model if ETL wasn't executed
            self._data_model = self._io.read_data_model(logger=self._logger)

        # Step 1: Initialize Problem
        self._logger = logging.getLogger("Problem")
        self._logger.info("Initializing the Problem with scope.")

        invoice_problem = InvoiceProblem(data_model=self._data_model)

        # Initialize the orchestrator with the register dataframe)
        self._logger = logging.getLogger("InvoiceOrchestrator")
        self._logger.info("Running the InvoiceOrchestrator.")
        orchestrator = InvoiceOrchestrator(
            problem=invoice_problem,
            config=self._config,
            openai_extractor=self._openai_extractor,
            ocr_extractor=self._ocr_extractor,
            sheets_manager=self._sheets_manager,
            gmail_manager=self._gmail_manager,
            drive_manager=self._drive_manager,
            logger=self._logger
        )

        # Run orchestration; the updated register DataFrame is returned.
        self._solution = orchestrator.run()
        self._logger.info(f"Solution generated with keys: {list(self._solution.keys())}")

        # Step 3: Export Solution (if enabled)
        if self._config.export_solution:
            if self._solution and any(not df.empty for df in self._solution.values()):
                self._logger.info("Exporting the solution DataFrames to the output directory.")
                self._io.write_solution_model(solution=self._solution, logger=self._logger)
            else:
                self._logger.warning("Solution dictionary is empty or all DataFrames are empty. Nothing to export.")


    def run(self):
        """
        Orchestrate the execution of ETL, Solver, and Post-Process.

        Logs the execution time for the complete process.
        """
        start_time = t.time()
        file_handler = self._setup_file_handler()

        try:
            if self._config.run_etl:
                self.run_etl()

            if self._config.run_solver:
                self.run_solver()

            self._logger.info(f"Execution completed in {t.time() - start_time:.2f} seconds.")

        finally:
            file_handler.close()


if __name__ == "__main__":
    warnings.filterwarnings("ignore", category=DeprecationWarning)
    main_process = MainProcess()
    main_process.run()
