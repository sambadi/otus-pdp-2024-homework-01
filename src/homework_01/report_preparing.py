import datetime
import gzip
import json
from pathlib import Path
import hashlib

import structlog
import os
from statistics import median
from collections import namedtuple

import re
from typing import Generator, Literal, Any
from dataclasses import field, dataclass

logger = structlog.get_logger()

LogInfo = namedtuple(
    "LogInfo", ["filename", "full_path", "hash", "log_date", "is_gzipped"]
)


@dataclass
class ParsingResult:
    row_process_error_cnt: int = field(default=0)
    req_total_count: int = field(default=0)
    req_total_time: float = field(default=0.0)
    stats: list[dict[str, Any]] = field(default_factory=list)

    @property
    def error_percent(self) -> float:
        return self.row_process_error_cnt / self.req_total_count * 100

    @property
    def has_data_to_render(self) -> bool:
        return self.stats is not None and len(self.stats) > 0


BASE_ENCODING = "utf-8"


def _calculate_hash(file_path) -> str:
    """
    Calculate the hash of a file.
    :param file_path: path to the file
    """
    with open(file_path, "rb") as f:
        file_hash = hashlib.md5()
        while chunk := f.read(8192):
            file_hash.update(chunk)

        return file_hash.hexdigest()


def _get_latest_log_info(log_location: str, log_name_pattern: str) -> LogInfo | None:
    """
    Get the latest log file in the given directory with the given name pattern.
    :param log_location: path to the log directory
    :param log_name_pattern: the pattern to match the log file name
    :return: the latest log file info or None
    """
    logger.info("Searching for the latest log file")
    found_file = None

    if not os.path.exists(log_location) or not os.path.isdir(log_location):
        logger.error("Log location %s does not exist", log_location)
        return None

    for filename in os.listdir(log_location):
        if re.match(log_name_pattern, filename):
            found_file = (
                filename if found_file is None or filename > found_file else found_file
            )

    if found_file is None:
        logger.info("No log files found in %s", log_location)
        return None

    file_parts = found_file.split(".")

    date = datetime.datetime.strptime(file_parts[1][4:], "%Y%m%d").date()
    is_gzipped = file_parts[-1] == "gz"

    logger.info("Found latest log file %s", os.path.join(log_location, found_file))

    full_path = os.path.join(log_location, found_file)

    return LogInfo(
        filename=found_file,
        full_path=full_path,
        log_date=date,
        hash=_calculate_hash(full_path),
        is_gzipped=is_gzipped,
    )


def _read_log_file(
    log_info: LogInfo, row_pattern: str
) -> Generator[dict[str, str] | Literal[False], None, None]:
    """
    Read the given log file and return a generator that yields each row of the log file.
    :param log_info: log file info
    :param row_pattern: pattern to match each row
    :return: generator that yields each row of the log file
    """
    if not os.path.exists(log_info.full_path):
        return None

    stream_type = gzip.open if log_info.is_gzipped else open
    row_pattern_cmp = re.compile(row_pattern)
    try:
        with stream_type(
            log_info.full_path, "rt", encoding=BASE_ENCODING
        ) as log_stream:
            for row in log_stream:
                matches = row_pattern_cmp.match(row)
                if matches is not None:
                    yield matches.groupdict()
                else:
                    yield False
    except UnicodeDecodeError:
        logger.error("File %s is not %s encoded", log_info.full_path, BASE_ENCODING)
        return None
    except OSError as e:
        logger.error("Error opening %s. Error: %s", log_info.full_path, e)
        return None


def _parse_log_file(
    log: Generator[dict[str, str] | Literal[False], None, None],
) -> ParsingResult:
    """
    Parse the given log file.
    :param log: log file generator
    :return: parsed log data
    """
    logger.info("Start log file processing...")

    result = ParsingResult()
    stats_map: dict[str, dict[str, Any]] = {}

    for row in log:
        result.req_total_count += 1

        if result.req_total_count % 100 == 0:
            logger.debug("Parsed %d requests...", result.req_total_count)

        if row is False:
            result.row_process_error_cnt += 1
            continue

        request_path = row["request_path"]
        request_time = float(row["request_time"])
        result.req_total_time += request_time

        request_stat = stats_map.setdefault(
            request_path,
            {
                "path": request_path,
                "time_sum": 0.0,
                "request_times": [],
                "count": 0,
                "time_max": 0.0,
            },
        )

        request_stat["time_sum"] += request_time
        request_stat["request_times"].append(request_time)
        if request_stat["time_max"] < request_time:
            request_stat["time_max"] = request_time
        request_stat["count"] += 1

    logger.info(
        f"{result.req_total_count} requests processed. {result.row_process_error_cnt} errors."
    )

    if not stats_map:
        return result

    result.stats = sorted(stats_map.values(), key=lambda x: x["time_max"], reverse=True)

    return result


def _prepare_report_data(
    parsed_log: ParsingResult,
    report_size: int,
    parse_error_threshold_percent: int = -1,
) -> list[dict]:
    """
    Prepare the report.
    :param parsed_log: result of parsing the log file
    :param report_size:  maximum count of requests to be rendered in the report
    :param parse_error_threshold_percent:  maximum percentage of requests that can be parsed with errors
    :return:  list of dicts that represent the report rows
    """
    if parsed_log.req_total_count > 0 and (
        0 < parse_error_threshold_percent <= parsed_log.error_percent
    ):
        raise ValueError(
            f"Too many errors (more than {parse_error_threshold_percent}%). Please check log file."
        )

    return [
        {
            "url": row["path"],
            "count": row["count"],
            "count_perc": round(row["count"] / parsed_log.req_total_count * 100, 5),
            "time_sum": round(row["time_sum"], 5),
            "time_max": row["time_max"],
            "time_perc": round(row["time_sum"] / parsed_log.req_total_time * 100, 5),
            "time_avg": round(row["time_sum"] / row["count"], 5),
            "time_med": median(row["request_times"]),
        }
        for row in parsed_log.stats[:report_size]
    ]


def _render_report(log_file_info: LogInfo, data_rows: list[dict], out_dir: str):
    logger.info("Start rendering report...")
    root_path = Path(__file__).parent.resolve()
    template_content = root_path.joinpath(
        "templates", "report_template.html"
    ).read_text(encoding=BASE_ENCODING)

    logger.debug("Template loaded...")
    if not os.path.isabs(out_dir):
        out_dir = os.path.join(os.getcwd(), out_dir)
    out_path = Path(out_dir)
    if not out_path.exists():
        out_path.mkdir(parents=True, exist_ok=True)
        logger.debug("Created output dir...")
    report_name = f"report-{log_file_info.log_date:%Y.%m.%d}.html"
    logger.info(f"Will write {len(data_rows)} rows to report to {out_path}...")
    out_path.joinpath(report_name).write_text(
        template_content.replace("$table_json", json.dumps(data_rows))
    )
    logger.info("Report %s was rendered...", report_name)


def _get_latest_parsed_file_hash(location: str) -> str | None:
    """
    Get latest parsed file hash from file.
    :param location: Directory where to get hash from.
    :return: Latest parsed file hash or None if not exists.
    """
    pth = Path(location).joinpath("last.parsed")
    if pth.exists():
        logger.debug("Latest parsed file hash found...")
        return pth.read_text()
    else:
        return None


def _save_latest_parsed_file_hash(location, log_file_info: LogInfo):
    """
    Save latest parsed file hash to file.
    :param location: Directory where to save hash.
    :param log_file_info: LogInfo object with parsed log file info.
    :return: None
    """
    pth = Path(location).joinpath("last.parsed")
    pth.write_text(log_file_info.hash)
    logger.debug("Latest parsed file hash saved...")


def prepare_report_based_on_latest_log_file(config: dict):
    """
    Prepare report based on latest log file.
    :param config:
    :return: None
    """
    logger.info("Start preparing report based on latest log file...")

    log_file_info = _get_latest_log_info(
        config["LOG_DIR"],
        config["LOG_FILE_NAME_PATTERN"],
    )

    if not log_file_info:
        logger.info("No log file found!")
        return

    report_dir = config["REPORT_DIR"]

    if not os.path.isabs(report_dir):
        report_dir = os.path.join(os.getcwd(), report_dir)

    last_parsed_file_hash = _get_latest_parsed_file_hash(location=report_dir)

    if last_parsed_file_hash and last_parsed_file_hash == log_file_info.hash:
        logger.info(f"File {log_file_info.full_path} is already parsed!")
        return

    parsed_log = _parse_log_file(
        log=_read_log_file(
            log_info=log_file_info, row_pattern=config["LOG_ROW_PATTERN"]
        ),
    )

    if not parsed_log.has_data_to_render:
        logger.info("Parsed log file has no data!")
        return

    report_data = _prepare_report_data(
        parsed_log=parsed_log,
        report_size=config["REPORT_SIZE"],
        parse_error_threshold_percent=int(
            config.get("PARSE_ERROR_THRESHOLD_PERCENT", -1)
        ),
    )

    if not parsed_log.has_data_to_render:
        logger.info("No data to render!")
        return

    _render_report(log_file_info, report_data, out_dir=report_dir)

    _save_latest_parsed_file_hash(location=report_dir, log_file_info=log_file_info)

    logger.info("Report is ready!")
