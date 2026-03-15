# =============================================================================
# src/utils/logger.py
#
# Logger structure pour tous les jobs PySpark.
#
# Pourquoi ne pas utiliser print() en production ?
#   - print() ne porte aucune information : pas de timestamp, pas de niveau,
#     pas de module source. Impossible de savoir quand et ou.
#   - print() n'est pas filtreable : on ne peut pas demander "montre moi
#     seulement les erreurs" a un outil de monitoring.
#   - print() ne s'integre pas avec Datadog, Splunk, CloudWatch, ELK...
#     Ces outils attendent du JSON : chaque ligne = un objet JSON parseable.
#
# Ce module produit des logs JSON comme :
#   {"timestamp":"2024-01-15T10:30:00.123Z","level":"INFO",
#    "logger":"etl_job","message":"Extraction terminee","rows":50000}
#
# Usage :
#   from src.utils.logger import get_logger
#   logger = get_logger(__name__)
#   logger.info("Job demarre", extra={"job": "orders_etl", "env": "prod"})
# =============================================================================

import json
import logging
import os
import sys
from datetime import datetime, timezone


class StructuredFormatter(logging.Formatter):
    """
    Formateur qui serialise chaque entree de log en une ligne JSON.

    Champs toujours presents :
      timestamp  - ISO-8601 UTC
      level      - DEBUG / INFO / WARNING / ERROR / CRITICAL
      logger     - nom du logger (chemin du module Python)
      message    - message de log
      module     - nom du fichier .py
      function   - nom de la fonction appelante
      line       - numero de ligne

    Champs optionnels (ajoutes via extra={}) :
      n'importe quelle cle/valeur passee dans extra={}
      ex: extra={"rows": 50000, "job": "orders_etl"}
    """

    def format(self, record: logging.LogRecord) -> str:
        # Champs de base toujours presents
        log_entry = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).strftime(
                "%Y-%m-%dT%H:%M:%S."
            )
            + f"{record.msecs:03.0f}Z",
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }

        # Ajouter les champs extra passes par l'appelant
        # (on exclut les attributs internes de LogRecord)
        _internal_attrs = {
            "name",
            "msg",
            "args",
            "levelname",
            "levelno",
            "pathname",
            "filename",
            "module",
            "exc_info",
            "exc_text",
            "stack_info",
            "lineno",
            "funcName",
            "created",
            "msecs",
            "relativeCreated",
            "thread",
            "threadName",
            "processName",
            "process",
            "message",
            "taskName",
        }
        for key, value in record.__dict__.items():
            if key not in _internal_attrs:
                log_entry[key] = value

        # Ajouter la stack trace si une exception est attachee
        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_entry, default=str, ensure_ascii=False)


class TextFormatter(logging.Formatter):
    """
    Formateur lisible par un humain pour le developpement local.
    Active avec LOG_FORMAT=text.

    Exemple de sortie :
      [2024-01-15 10:30:00] INFO  etl_job:82 - Extraction terminee | rows=50000
    """

    LEVEL_COLORS = {
        "DEBUG": "\033[36m",  # cyan
        "INFO": "\033[32m",  # vert
        "WARNING": "\033[33m",  # jaune
        "ERROR": "\033[31m",  # rouge
        "CRITICAL": "\033[35m",  # magenta
    }
    RESET = "\033[0m"

    def format(self, record: logging.LogRecord) -> str:
        ts = datetime.fromtimestamp(record.created).strftime("%Y-%m-%d %H:%M:%S")
        level = record.levelname.ljust(8)
        location = f"{record.module}:{record.lineno}"
        message = record.getMessage()

        # Extraire les champs extra
        _internal_attrs = {
            "name",
            "msg",
            "args",
            "levelname",
            "levelno",
            "pathname",
            "filename",
            "module",
            "exc_info",
            "exc_text",
            "stack_info",
            "lineno",
            "funcName",
            "created",
            "msecs",
            "relativeCreated",
            "thread",
            "threadName",
            "processName",
            "process",
            "message",
            "taskName",
        }
        extras = {k: v for k, v in record.__dict__.items() if k not in _internal_attrs}
        extra_str = "  |  " + "  ".join(f"{k}={v}" for k, v in extras.items()) if extras else ""

        # Colorisation (seulement si le terminal le supporte)
        use_color = hasattr(sys.stderr, "isatty") and sys.stderr.isatty()
        if use_color:
            color = self.LEVEL_COLORS.get(record.levelname, "")
            line = f"[{ts}] {color}{level}{self.RESET} {location} - {message}{extra_str}"
        else:
            line = f"[{ts}] {level} {location} - {message}{extra_str}"

        if record.exc_info:
            line += "\n" + self.formatException(record.exc_info)

        return line


def get_logger(name: str, level: str | None = None) -> logging.Logger:
    """
    Cree ou recupere un logger configure pour ce projet.

    Idempotent : appeler get_logger("x") deux fois retourne le meme logger
    sans dupliquer les handlers (important en Spark ou plusieurs modules
    importent le logger).

    Args:
        name  : nom du logger, utiliser __name__ (ex: "src.jobs.etl_job")
        level : niveau de log. Par defaut : variable d'env LOG_LEVEL ou INFO.

    Returns:
        logging.Logger configure avec le bon formateur.

    Variables d'environnement :
        LOG_LEVEL  : DEBUG | INFO | WARNING | ERROR  (defaut: INFO)
        LOG_FORMAT : json | text                     (defaut: json)
    """
    logger = logging.getLogger(name)

    # Idempotence : ne pas ajouter de handlers si deja configure
    if logger.handlers:
        return logger

    # Niveau de log
    log_level_str = level or os.getenv("LOG_LEVEL", "INFO").upper()
    log_level = getattr(logging, log_level_str, logging.INFO)
    logger.setLevel(log_level)

    # Choix du formateur
    log_format = os.getenv("LOG_FORMAT", "json").lower()
    formatter = TextFormatter() if log_format == "text" else StructuredFormatter()

    # Handler vers stderr (standard pour les applications containerisees)
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    # Ne pas propager vers le logger racine (evite les doublons)
    logger.propagate = False

    return logger
