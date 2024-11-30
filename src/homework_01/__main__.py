import json
import logging
import os.path
from pathlib import Path

import argparse
import structlog
import sys
from homework_01.report_preparing import prepare_report_based_on_latest_log_file

logger = structlog.get_logger()

_default_config = {
    "SELF_LOG_FILE_PATH": None,
    "PARSE_ERROR_THRESHOLD_PERCENT": 10,
    "REPORT_SIZE": 1000,
    "REPORT_DIR": "./reports",
    "LOG_DIR": "./log",
    "LOG_FILE_NAME_PATTERN": "nginx-access-ui.log.*",
    "LOG_ROW_PATTERN": (
        r"^(?P<remote_addr>[^\s]*)\s+(?P<remote_user>[^\s]*)\s+(?P<http_x_real_ip>[^\s]*)\s+\[(?P<time_local>.*)\]\s+"
        r"\"(?P<request_method>[^\s]*)\s+(?P<request_path>[^\s]*)\s+(?P<request_http_version>[^\s]*)\"\s+"
        r"(?P<status>[^\s]*)\s+(?P<body_bytes_sent>[^\s]*)\s+\"(?P<http_referer>[^\"]*)\"\s+"
        r"\"(?P<http_user_agent>[^\"]*)\"\s+\"(?P<http_x_forwarded_for>[^\"]*)\"\s+\"(?P<http_X_REQUEST_ID>[^\"]*)\"\s+"
        r"\"(?P<http_X_RB_USER>[^\"]*)\"\s+(?P<request_time>.*)$"
    ),
}


def init_logging(log_file_path: str | None):
    if log_file_path:
        Path(log_file_path).parent.mkdir(parents=True, exist_ok=True)
        logging.basicConfig(
            filename=log_file_path,
            encoding="utf-8",
            level=logging.DEBUG,
            format="%(message)s",
        )
    else:
        logging.basicConfig(
            format="%(message)s",
            stream=sys.stdout,
            level=logging.DEBUG,
        )
    structlog.configure(
        processors=[
            # If log level is too low, abort pipeline and throw away log entry.
            structlog.stdlib.filter_by_level,
            # Add the name of the logger to event dict.
            structlog.stdlib.add_logger_name,
            # Add log level to event dict.
            structlog.stdlib.add_log_level,
            # Perform %-style formatting.
            structlog.stdlib.PositionalArgumentsFormatter(),
            # Add a timestamp in ISO 8601 format.
            structlog.processors.TimeStamper(fmt="iso"),
            # If the "stack_info" key in the event dict is true, remove it and
            # render the current stack trace in the "stack" key.
            structlog.processors.StackInfoRenderer(),
            # If the "exc_info" key in the event dict is either true or a
            # sys.exc_info() tuple, remove "exc_info" and render the exception
            # with traceback into the "exception" key.
            structlog.processors.format_exc_info,
            # If some value is in bytes, decode it to a Unicode str.
            structlog.processors.UnicodeDecoder(),
            # Add callsite parameters.
            structlog.processors.CallsiteParameterAdder(
                {
                    structlog.processors.CallsiteParameter.FILENAME,
                    structlog.processors.CallsiteParameter.FUNC_NAME,
                    structlog.processors.CallsiteParameter.LINENO,
                }
            ),
            # Render the final event dict as JSON.
            structlog.processors.JSONRenderer(),
        ],
        # `wrapper_class` is the bound logger that you get back from
        # get_logger(). This one imitates the API of `logging.Logger`.
        wrapper_class=structlog.stdlib.BoundLogger,
        # `logger_factory` is used to create wrapped loggers that are used for
        # OUTPUT. This one returns a `logging.Logger`. The final value (a JSON
        # string) from the final processor (`JSONRenderer`) will be passed to
        # the method of the same name as that you've called on the bound logger.
        logger_factory=structlog.stdlib.LoggerFactory(),
        # Effectively freeze configuration after creating the first bound
        # logger.
        cache_logger_on_first_use=True,
    )


if __name__ == "__main__":
    try:
        parser = argparse.ArgumentParser("homework 01")

        parser.add_argument(
            "-c",
            "--config",
            help="path to config file",
            type=str,
            default="config.json",
        )
        args = parser.parse_args()

        config_arg_value = args.config
        if not os.path.isabs(config_arg_value):
            config_arg_value = os.path.join(os.getcwd(), args.config)
        cfg_path = Path(config_arg_value)

        if not cfg_path.exists():
            sys.exit("Config file does not exist")
        try:
            cfg = json.loads(cfg_path.read_text(encoding="utf-8"))
            config = _default_config.copy()
            config.update(cfg)
            init_logging(cfg.get("SELF_LOG_FILE_PATH"))
        except Exception:
            sys.exit(f"Could not parse config file {args.config}")

        prepare_report_based_on_latest_log_file(config)
    except BaseException as e:
        logger.exception(f"Unexpected error: {e}")
        sys.exit()
