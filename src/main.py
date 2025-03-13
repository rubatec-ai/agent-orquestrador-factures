import logging
import os
import time as t
import io
from datetime import datetime

from src.config import ConfigurationManager
from src.data_control import DataController
from src.io_methods import IOHandler
from src.transform import Transformer
from src.post_process import PostProcessor

from src.ai_summarizer.classes.problem import Problem
from src.ai_summarizer.classes.agent import Agent
from src.ai_summarizer.classes.semantic_search import SemanticSearch
import warnings


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

    def __init__(self, streamlit=False):
        self._config = ConfigurationManager(streamlit=streamlit)
        self._io = IOHandler(self._config)
        self._logger, self._log_stream = self._initialize_logger(streamlit)

        self._data_model = None
        self._problem = None

    def _initialize_logger(self, streamlit: bool) -> tuple[logging.Logger, io.StringIO | None]:
        """
        Configure the logger instance.

        Args:
            streamlit (bool): Whether to integrate with Streamlit.

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

        # Mute external loggers
        logging.getLogger("openai._base_client").setLevel(logging.WARNING)

        logging.getLogger("pdfminer.pdfparser").setLevel(logging.WARNING)
        logging.getLogger("pdfminer.pdfpage").setLevel(logging.WARNING)
        logging.getLogger("pdfminer.psparser").setLevel(logging.WARNING)
        logging.getLogger("pdfminer.cmapdb").setLevel(logging.WARNING)
        logging.getLogger("pdfminer.pdfinterp").setLevel(logging.WARNING)
        logging.getLogger("pdfminer.pdfdocument").setLevel(logging.WARNING)

        logging.getLogger("httpcore.http11").setLevel(logging.WARNING)
        logging.getLogger("httpcore.connection").setLevel(logging.WARNING)

        # Add a StringIO handler for Streamlit if enabled
        log_stream = None
        if streamlit:
            log_stream = io.StringIO()
            stream_handler = logging.StreamHandler(log_stream)
            stream_handler.setLevel(log_level)
            stream_handler.setFormatter(logging.Formatter(
                "%(asctime)s - %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S"
            ))
            logger.addHandler(stream_handler)

        return logger, log_stream

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

        Reads input data, applies transformations, and saves the resulting data model.
        """
        data_controller = DataController(config=self._config, io_handler=self._io, logger=self._logger)
        data_controller.load_inputs()

        transformer = Transformer(config=self._config, io_handler=self._io, logger=self._logger,
                                   clean_inputs=data_controller.processed_inputs)
        self._data_model = transformer.run()

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
        self._logger.name = 'Problem'
        self._logger.info("Initializing the Problem with scope and master data.")

        problem = Problem(scope_df=self._data_model["param_master"],master_df= self._data_model["text_master"])

        # Step 2: Initialize Agent
        self._logger.name = 'Agent'
        self._logger.info("Initializing the Agent for semantic search.")
        agent = Agent(
            model=self._config.agent_model,  # Replace with your configured model
            api_key=self._config.agent_api_key,  # API key from configuration
            temperature=self._config.agent_temperature,
            max_tokens=self._config.agent_max_tokens,
            logger=self._logger
        )

        # Step 3: Perform Semantic Search
        self._logger.name = 'SemanticSearch'
        self._logger.info("Running the SemanticSearch process.")
        semantic_search = SemanticSearch(config=self._config, problem=problem, agent=agent, logger= self._logger)
        semantic_search.run_search()

        # Step 4: Generate Output DataFrame
        self._logger.info("Generating the solution DataFrame from SemanticSearch results.")
        self._data_model["solution"] = semantic_search.generate_output_dataframe()

        # Step 5: Export Solution (if enabled)
        if self._config.export_solution:
            if not self._data_model["solution"].empty:
                self._logger.info("Exporting the solution DataFrame to the output directory.")
                self._io.write_solution(self._data_model["solution"])
            else:
                self._logger.warning("Solution DataFrame is empty. Nothing to export.")

    def run_post_process(self) -> None:
        """
        Execute the post-processing step.

        Prepares and exports post-processed data based on the solution.
        """
        if not self._config.run_etl:
            self._data_model = self._io.read_data_model(logger=self._logger)

        if not self._config.run_solver:
            self._data_model['solution'] = self._io.read_solution(logger=self._logger)

        post_processor = PostProcessor(data_model=self._data_model, config=self._config, logger=self._logger,
                                       io_handler=self._io)
        post_processor.run()

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

            if self._config.run_post_process:
                self.run_post_process()

            self._logger.info(f"Execution completed in {t.time() - start_time:.2f} seconds.")

        finally:
            file_handler.close()

if __name__ == "__main__":
    warnings.filterwarnings("ignore", category=DeprecationWarning)
    main_process = MainProcess()
    main_process.run()